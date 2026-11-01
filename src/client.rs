use crate::datum::Datum;
use crate::error::S7Error;
use crate::item::{DataItem, RequestItem, S7ReturnItem};
use crate::packet::{S7Data, S7DataCodec};
use crate::paramter::S7Parameter;
use crate::types::{
    CommunicationInfo, CpuInfo, OrderCode, PduType, PlcStatus, PlcType, S7Area, S7ErrorClass,
    S7ReturnCode,
};
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::sync::{mpsc, oneshot};
use tokio::time::sleep;
use tokio_util::bytes::{Buf, BytesMut};
use tokio_util::codec::Framed;

/// S7 connection configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct S7Config {

    pub name: String,
    /// Host address
    pub host: String,
    /// Port (default 102)
    pub port: u16,
    /// PLC type
    pub plc_type: PlcType,
    /// Connection type (PG/OP/S7)
    pub connection_type: u8,
    /// Rack number
    pub rack: u8,
    /// Slot number
    pub slot: u8,
    /// Timeout in milliseconds
    pub timeout_ms: u64,
    /// Maximum PDU size
    pub max_pdu_size: u16,

    pub auto_reconnect: bool,
}

impl Default for S7Config {
    fn default() -> Self {
        Self {
            name: "tokio-s7".to_string(),
            host: "127.0.0.1".to_string(),
            port: 102,
            plc_type: PlcType::S1200,
            connection_type: 0x01, // PG
            rack: 0,
            slot: 2,
            timeout_ms: 5000,
            max_pdu_size: 960,
            auto_reconnect: false,
        }
    }
}

impl S7Config {
    pub fn new(host: &str) -> Self {
        Self {
            host: host.to_string(),
            ..Default::default()
        }
    }

    /// Set PLC type
    pub fn with_plc_type(mut self, plc_type: PlcType) -> Self {
        let (rack, slot) = plc_type.default_rack_slot();
        self.plc_type = plc_type;
        self.rack = rack;
        self.slot = slot;
        self
    }

    /// Set rack and slot
    pub fn with_rack_slot(mut self, rack: u8, slot: u8) -> Self {
        self.rack = rack;
        self.slot = slot;
        self
    }

    /// Set connection type
    pub fn with_connection_type(mut self, conn_type: u8) -> Self {
        self.connection_type = conn_type;
        self
    }

    /// Set port
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, timeout_ms: u64) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    pub fn with_auto_reconnect(mut self, auto_reconnect: bool) -> Self {
        self.auto_reconnect = auto_reconnect;
        self
    }
}

/// S7 connection state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// Connecting
    Connecting,
    /// Connected but not negotiated
    Connected,
    /// Connection established and negotiated
    Negotiated,
    /// Communicating
    Communicating,
}

// 命令枚举
pub enum Command {
    Read {
        items: Vec<RequestItem>,
        reply: oneshot::Sender<Result<Vec<DataItem>, S7Error>>,
    },
    Write {
        items: Vec<RequestItem>,
        data: Vec<DataItem>,
        reply: oneshot::Sender<Result<(), S7Error>>,
    },
    ReadSzl {
        szl_id: u16,
        szl_index: u16,
        reply: oneshot::Sender<Result<S7Data, S7Error>>,
    },
    Disconnect {
        reply: oneshot::Sender<Result<(), S7Error>>,
    },
}

#[derive(Debug)]
pub struct S7Client {
    pub pdu_length: u16,
    pub connection_state: ConnectionState,
    pub config: S7Config,
    pub cmd_tx: Option<mpsc::Sender<Command>>,
}

impl S7Client {
    pub fn new(config: S7Config) -> Self {
        Self {
            pdu_length: config.max_pdu_size,
            connection_state: ConnectionState::Disconnected,
            config,
            cmd_tx: None,
        }
    }

    /// 优雅断开与 PLC 的连接（发送 COTP DR，关闭 TCP），停止后台 actor。
    pub async fn disconnect(&mut self) -> Result<(), S7Error> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| S7Error::Error("Not connected".into()))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(Command::Disconnect { reply: reply_tx })
            .await
            .map_err(|_| S7Error::Error("Actor disconnected".into()))?;
        let result = reply_rx
            .await
            .map_err(|_| S7Error::Error("Actor dropped".into()))?;
        // 标记状态为已断开，并清除发送端（actor 已退出）
        self.connection_state = ConnectionState::Disconnected;
        self.cmd_tx = None;
        result
    }

    pub async fn connect(&mut self) -> Result<(), S7Error> {
        // 通道：用于接收握手结果（协商后的 PDU 长度）
        let (result_tx, result_rx) = oneshot::channel();
        let (cmd_tx, cmd_rx) = mpsc::channel(32);
        self.cmd_tx = Some(cmd_tx);

        // 启动后台 Actor
        tokio::spawn(run_actor(
            self.config.clone(),
            self.pdu_length,
            cmd_rx,
            result_tx,
        ));

        // 等待握手结果
        match result_rx.await {
            Ok(negotiated) => {
                self.pdu_length = negotiated?;
                self.connection_state = ConnectionState::Negotiated;
                info!("S7 connection established, PDU length: {}", self.pdu_length);
                Ok(())
            }
            Err(_) => {
                self.connection_state = ConnectionState::Disconnected;
                Err(S7Error::Error("Actor dropped before handshake".into()))
            }
        }
    }

    /// 异步读取数据（内部通过 Actor 通道）
    pub async fn read(&self, items: Vec<RequestItem>) -> Result<Vec<DataItem>, S7Error> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| S7Error::Error("Not connected".into()))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(Command::Read {
            items,
            reply: reply_tx,
        })
        .await
        .map_err(|_| S7Error::Error("Actor disconnected".into()))?;
        reply_rx
            .await
            .map_err(|_| S7Error::Error("Actor dropped".into()))?
    }

    /// 异步写入数据
    pub async fn write(&self, items: Vec<RequestItem>, data: Vec<DataItem>) -> Result<(), S7Error> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| S7Error::Error("Not connected".into()))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(Command::Write {
            items,
            data,
            reply: reply_tx,
        })
        .await
        .map_err(|_| S7Error::Error("Actor disconnected".into()))?;
        reply_rx
            .await
            .map_err(|_| S7Error::Error("Actor dropped".into()))?
    }

    pub async fn read_byte(&self, address: &str) -> Result<u8, S7Error> {
        let bytes = self.read_bytes(address, 1).await?;
        bytes
            .first()
            .copied()
            .ok_or_else(|| S7Error::Error("read_byte: empty response".into()))
    }

    pub async fn read_sbyte(&self, address: &str) -> Result<i8, S7Error> {
        let byte = self.read_byte(address).await?;
        Ok(byte as i8)
    }

    pub async fn write_byte(&self, address: &str, data: u8) -> Result<(), S7Error> {
        let req_item = RequestItem::parse_byte(address, 1)?;
        let data_item = DataItem::create_req_bytes(vec![data])?;
        self.write(vec![req_item], vec![data_item]).await
    }

    pub async fn read_bytes(&self, address: &str, count: u16) -> Result<Vec<u8>, S7Error> {
        let req_item = RequestItem::parse_byte(address, count)?;
        let data_items = self.read(vec![req_item]).await?;
        data_items
            .into_iter()
            .next()
            .map(|item| item.data)
            .ok_or_else(|| S7Error::Error("read_bytes: no data item returned".into()))
    }

    /// 读取有符号字节数组（每个字节解释为 i8）
    pub async fn read_sbytes(&self, address: &str, count: u16) -> Result<Vec<i8>, S7Error> {
        let bytes = self.read_bytes(address, count).await?;
        Ok(bytes.into_iter().map(|b| b as i8).collect())
    }

    pub async fn write_bytes(&self, address: &str, data: &[u8]) -> Result<(), S7Error> {
        let req_item = RequestItem::parse_byte(address, data.len() as u16)?;
        let data_item = DataItem::create_req_bytes(data.to_vec())?;
        self.write(vec![req_item], vec![data_item]).await
    }

    /// 读取一个位，例如 `DB10.5.3`、`M1.2`
    pub async fn read_bool(&self, address: &str) -> Result<bool, S7Error> {
        let req_item = RequestItem::parse_bit(address)
            .map_err(|e| S7Error::Error(format!("Address parse error: {}", e)))?;
        let data_items = self.read(vec![req_item]).await?;
        let byte = data_items
            .first()
            .and_then(|item| item.data.first())
            .copied()
            .ok_or_else(|| S7Error::Error("read_bool: empty response".into()))?;

        Ok(((byte & 0xFF) & (1 << 0)) != 0)
    }

    /// 写入一个位（例如 "DB10.5.3" 或 "M1.2"）
    /// 注意：此操作直接写入包含该位的整个字节，位值由 `bit_address` 和 `value` 决定。
    /// 其他位会被置为 0（若不需要影响其他位，请在写入前先读取并修改）。
    pub async fn write_bool(&self, address: &str, value: bool) -> Result<(), S7Error> {
        let req = RequestItem::parse_bit(address)
            .map_err(|e| S7Error::Error(format!("Address parse error: {}", e)))?;
        let data = DataItem::create_req_bool(value)?;
        self.write(vec![req], vec![data]).await
    }

    // ---------- 16-bit signed/unsigned ----------

    pub async fn read_int16(&self, address: &str) -> Result<i16, S7Error> {
        let bytes = self.read_bytes(address, 2).await?;
        Ok(i16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub async fn read_uint16(&self, address: &str) -> Result<u16, S7Error> {
        let bytes = self.read_bytes(address, 2).await?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }

    pub async fn read_int16_le(&self, address: &str) -> Result<i16, S7Error> {
        let bytes = self.read_bytes(address, 2).await?;
        Ok(i16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub async fn read_uint16_le(&self, address: &str) -> Result<u16, S7Error> {
        let bytes = self.read_bytes(address, 2).await?;
        Ok(u16::from_le_bytes([bytes[0], bytes[1]]))
    }

    pub async fn write_int16(&self, address: &str, data: i16) -> Result<(), S7Error> {
        let bytes = data.to_be_bytes();
        let req_item = RequestItem::parse_byte(address, 2)?;
        let data_item = DataItem::create_req_bytes(bytes.to_vec())?;
        self.write(vec![req_item], vec![data_item]).await
    }

    pub async fn write_uint16(&self, address: &str, data: u16) -> Result<(), S7Error> {
        let bytes = data.to_be_bytes();
        let req_item = RequestItem::parse_byte(address, 2)?;
        let data_item = DataItem::create_req_bytes(bytes.to_vec())?;
        self.write(vec![req_item], vec![data_item]).await
    }

    pub async fn write_int16_le(&self, address: &str, value: i16) -> Result<(), S7Error> {
        let bytes = value.to_le_bytes();
        self.write_bytes(address, &bytes).await
    }

    pub async fn write_uint16_le(&self, address: &str, value: u16) -> Result<(), S7Error> {
        let bytes = value.to_le_bytes();
        self.write_bytes(address, &bytes).await
    }

    // ---------- 32-bit signed/unsigned ----------

    pub async fn read_int32(&self, address: &str) -> Result<i32, S7Error> {
        let bytes = self.read_bytes(address, 4).await?;
        Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    /// S7 的 UInt32 直接返回 u32（0 ~ 4_294_967_295）
    pub async fn read_uint32(&self, address: &str) -> Result<u32, S7Error> {
        let bytes = self.read_bytes(address, 4).await?;
        Ok(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub async fn read_int32_le(&self, address: &str) -> Result<i32, S7Error> {
        let bytes = self.read_bytes(address, 4).await?;
        Ok(i32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub async fn read_uint32_le(&self, address: &str) -> Result<u32, S7Error> {
        let bytes = self.read_bytes(address, 4).await?;
        Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub async fn write_int32(&self, address: &str, value: i32) -> Result<(), S7Error> {
        let req = RequestItem::parse_byte(address, 4)
            .map_err(|e| S7Error::Error(format!("Address parse error: {}", e)))?;
        let data = DataItem::create_req_bytes(value.to_be_bytes().to_vec())?;
        self.write(vec![req], vec![data]).await
    }

    pub async fn write_uint32(&self, address: &str, value: u32) -> Result<(), S7Error> {
        let req = RequestItem::parse_byte(address, 4)
            .map_err(|e| S7Error::Error(format!("Address parse error: {}", e)))?;
        let data = DataItem::create_req_bytes(value.to_be_bytes().to_vec())?;
        self.write(vec![req], vec![data]).await
    }

    pub async fn write_int32_le(&self, address: &str, value: i32) -> Result<(), S7Error> {
        let bytes = value.to_le_bytes();
        self.write_bytes(address, &bytes).await
    }

    pub async fn write_uint32_le(&self, address: &str, value: u32) -> Result<(), S7Error> {
        let bytes = value.to_le_bytes();
        self.write_bytes(address, &bytes).await
    }


    // ---------- 64-bit signed/unsigned ----------

    pub async fn read_int64(&self, address: &str) -> Result<i64, S7Error> {
        let bytes = self.read_bytes(address, 8).await?;
        Ok(i64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub async fn read_uint64(&self, address: &str) -> Result<u64, S7Error> {
        let bytes = self.read_bytes(address, 8).await?;
        Ok(u64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub async fn read_int64_le(&self, address: &str) -> Result<i64, S7Error> {
        let bytes = self.read_bytes(address, 8).await?;
        Ok(i64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub async fn read_uint64_le(&self, address: &str) -> Result<u64, S7Error> {
        let bytes = self.read_bytes(address, 8).await?;
        Ok(u64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub async fn write_int64(&self, address: &str, value: i64) -> Result<(), S7Error> {
        let req = RequestItem::parse_byte(address, 8)
            .map_err(|e| S7Error::Error(format!("Address parse error: {}", e)))?;
        let data = DataItem::create_req_bytes(value.to_be_bytes().to_vec())?;
        self.write(vec![req], vec![data]).await
    }

    pub async fn write_uint64(&self, address: &str, value: u64) -> Result<(), S7Error> {
        let req = RequestItem::parse_byte(address, 8)
            .map_err(|e| S7Error::Error(format!("Address parse error: {}", e)))?;
        let data = DataItem::create_req_bytes(value.to_be_bytes().to_vec())?;
        self.write(vec![req], vec![data]).await
    }

    pub async fn write_int64_le(&self, address: &str, value: i64) -> Result<(), S7Error> {
        let bytes = value.to_le_bytes();
        self.write_bytes(address, &bytes).await
    }

    pub async fn write_uint64_le(&self, address: &str, value: u64) -> Result<(), S7Error> {
        let bytes = value.to_le_bytes();
        self.write_bytes(address, &bytes).await
    }

    // ---------- Float (32-bit) / Double (64-bit) ----------

    pub async fn read_float32(&self, address: &str) -> Result<f32, S7Error> {
        let bytes = self.read_bytes(address, 4).await?;
        Ok(f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub async fn read_float64(&self, address: &str) -> Result<f64, S7Error> {
        let bytes = self.read_bytes(address, 8).await?;
        Ok(f64::from_be_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub async fn read_float32_le(&self, address: &str) -> Result<f32, S7Error> {
        let bytes = self.read_bytes(address, 4).await?;
        Ok(f32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
    }

    pub async fn read_float64_le(&self, address: &str) -> Result<f64, S7Error> {
        let bytes = self.read_bytes(address, 8).await?;
        Ok(f64::from_le_bytes([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5], bytes[6], bytes[7],
        ]))
    }

    pub async fn write_float32(&self, address: &str, value: f32) -> Result<(), S7Error> {
        let req = RequestItem::parse_byte(address, 4)
            .map_err(|e| S7Error::Error(format!("Address parse error: {}", e)))?;
        let data = DataItem::create_req_bytes(value.to_be_bytes().to_vec())?;
        self.write(vec![req], vec![data]).await
    }

    pub async fn write_float64(&self, address: &str, value: f64) -> Result<(), S7Error> {
        let req = RequestItem::parse_byte(address, 8)
            .map_err(|e| S7Error::Error(format!("Address parse error: {}", e)))?;
        let data = DataItem::create_req_bytes(value.to_be_bytes().to_vec())?;
        self.write(vec![req], vec![data]).await
    }

    pub async fn write_float32_le(&self, address: &str, value: f32) -> Result<(), S7Error> {
        let bytes = value.to_le_bytes();
        self.write_bytes(address, &bytes).await
    }

    pub async fn write_float64_le(&self, address: &str, value: f64) -> Result<(), S7Error> {
        let bytes = value.to_le_bytes();
        self.write_bytes(address, &bytes).await
    }

    // ---------- STRING / WSTRING ----------

    /// 读取西门子 STRING 类型（ASCII/ISO-8859-1）
    /// `max_len` 为声明最大字符数，默认为 254。
    pub async fn read_string(&self, address: &str, max_len: u16) -> Result<String, S7Error> {
        // STRING 存储格式：max_len(1B) + actual_len(1B) + chars(actual_len B)
        let total_len = max_len + 2;
        let bytes = self.read_bytes(address, total_len).await?;
        if bytes.len() < 2 {
            return Err(S7Error::Error("read_string: response too short".into()));
        }
        let actual_len = bytes[1] as usize;
        if bytes.len() < 2 + actual_len {
            return Err(S7Error::Error("read_string: data truncated".into()));
        }
        let chars = &bytes[2..2 + actual_len];
        // STRING 是 ISO-8859-1（Latin-1）：每个字节直接映射到同码位字符（U+0000..U+00FF），
        // 与写侧 `str_to_latin1` 对称。`from_utf8_lossy` 会把 0x80-0xFF 全部变成 U+FFFD。
        Ok(chars.iter().map(|&b| b as char).collect())
    }

    /// 读取西门子 STRING 类型（默认最大长度 254）
    pub async fn read_string_default(&self, address: &str) -> Result<String, S7Error> {
        self.read_string(address, 254).await
    }

    /// 读取西门子 WSTRING 类型（UTF-16BE）
    /// `max_len` 为声明最大字符数，默认为 254。
    pub async fn read_wstring(&self, address: &str, max_len: u16) -> Result<String, S7Error> {
        let total_len = max_len * 2 + 4;
        let bytes = self.read_bytes(address, total_len).await?;
        if bytes.len() < 4 {
            return Err(S7Error::Error("read_wstring: response too short".into()));
        }
        let max_len_resp = u16::from_be_bytes([bytes[0], bytes[1]]) as usize;
        let mut actual_len = u16::from_be_bytes([bytes[2], bytes[3]]) as usize;
        if actual_len > max_len_resp {
            actual_len = max_len_resp;
        }
        let data_start = 4;
        let data_end = data_start + actual_len * 2;
        if bytes.len() < data_end {
            return Err(S7Error::Error("read_wstring: data truncated".into()));
        }

        // 将 UTF-16BE 字节流转换为 u16 数组
        let u16s: Vec<u16> = bytes[data_start..data_end]
            .chunks_exact(2) // 将数据区按每 2 字节一组分割。
            .map(|chunk| u16::from_be_bytes([chunk[0], chunk[1]])) // 对于每组 2 字节，用 u16::from_be_bytes 组装成大端序的 16 位无符号整数（UTF‑16 代码单元）
            .collect();

        String::from_utf16(&u16s)
            .map_err(|_| S7Error::Error("read_wstring: invalid UTF-16 data".into()))
    }

    /// 读取西门子 WSTRING 类型（默认最大长度 254）
    pub async fn read_wstring_default(&self, address: &str) -> Result<String, S7Error> {
        self.read_wstring(address, 254).await
    }

    /// 写入西门子 STRING（Latin-1 编码，不支持中文）
    /// `max_len` 为 PLC 中声明的最大字符数。
    /// 若字符串长度超过 `max_len` 或包含无法编码的字符，返回错误。
    pub async fn write_string(
        &self,
        address: &str,
        max_len: u16,
        value: &str,
    ) -> Result<(), S7Error> {
        let latin1_bytes = str_to_latin1(value)?;
        let actual_len = latin1_bytes.len() as u16;
        if actual_len > max_len {
            return Err(S7Error::Error(format!(
                "String too long: {} bytes, max allowed {}",
                actual_len, max_len
            )));
        }
        // 构造 STRING 格式字节：max_len(1B) + actual_len(1B) + data
        let total_len = (max_len + 2) as usize;
        let mut buf = vec![0u8; total_len];
        buf[0] = max_len as u8;
        buf[1] = actual_len as u8;
        buf[2..2 + actual_len as usize].copy_from_slice(&latin1_bytes);
        // 剩余部分已经是 0
        let req = RequestItem::parse_byte(address, total_len as u16)
            .map_err(|e| S7Error::Error(format!("Address parse error: {}", e)))?;
        let data = DataItem::create_req_bytes(buf)?;
        self.write(vec![req], vec![data]).await
    }

    /// 写入西门子 WSTRING（UTF-16BE 编码，支持所有 Unicode 字符）
    /// `max_len` 为 PLC 中声明的最大字符数（不是字节数）。
    /// 若字符串代码单元数超过 `max_len`，返回错误。
    pub async fn write_wstring(
        &self,
        address: &str,
        max_len: u16,
        value: &str,
    ) -> Result<(), S7Error> {
        // 获取 UTF-16 代码单元数（注意：一个字符可能对应 1 个或 2 个代码单元）
        let utf16_units: Vec<u16> = value.encode_utf16().collect();
        let actual_len = utf16_units.len() as u16;
        if actual_len > max_len {
            return Err(S7Error::Error(format!(
                "WString too long: {} code units, max allowed {}",
                actual_len, max_len
            )));
        }
        // 构造 WSTRING 字节：max_len(2B big-endian) + actual_len(2B big-endian) + UTF-16BE 数据
        let total_bytes = (max_len * 2 + 4) as usize;
        let mut buf = vec![0u8; total_bytes];
        buf[0..2].copy_from_slice(&max_len.to_be_bytes());
        buf[2..4].copy_from_slice(&actual_len.to_be_bytes());
        for (i, unit) in utf16_units.iter().enumerate() {
            let pos = 4 + i * 2;
            buf[pos..pos + 2].copy_from_slice(&unit.to_be_bytes());
        }
        // 剩余位置保持 0
        let req = RequestItem::parse_byte(address, total_bytes as u16)
            .map_err(|e| S7Error::Error(format!("Address parse error: {}", e)))?;
        let data = DataItem::create_req_bytes(buf)?;
        self.write(vec![req], vec![data]).await
    }

    pub async fn szl_read(&self, szl_id: u16, szl_index: u16) -> Result<S7Data, S7Error> {
        let tx = self
            .cmd_tx
            .as_ref()
            .ok_or_else(|| S7Error::Error("Not connected".into()))?;
        let (reply_tx, reply_rx) = oneshot::channel();
        tx.send(Command::ReadSzl {
            szl_id,
            szl_index,
            reply: reply_tx,
        })
        .await
        .map_err(|_| S7Error::Error("Actor disconnected".into()))?;
        reply_rx
            .await
            .map_err(|_| S7Error::Error("Actor dropped".into()))?
    }

    pub async fn get_order_code(&self) -> Result<OrderCode, S7Error> {
        let resp_data = self.szl_read(0x0011, 0x0000).await?;
        match resp_data.datum {
            None => Err(S7Error::Error("Response has no any data".into()))?,
            Some(d) => {
                match d {
                    Datum::Szl(s) => {
                        let payload = s.all_data.unwrap();
                        let n = payload.len();
                        let (v1, v2, v3) = if n >= 3 {
                            (payload[n - 3], payload[n - 2], payload[n - 1])
                        } else {
                            (0, 0, 0)
                        };

                        let szl_id = s.szl_id;
                        let _szl_index = s.szl_index;
                        let entry_len = s.partlist_byte_len.unwrap() as usize;
                        let entry_count = s.partlist_count.unwrap() as usize;

                        let mut buf = BytesMut::from(payload.as_slice());
                        if szl_id == 0x0011 && entry_len >= 4 && entry_count > 0 {
                            for _ in 0..entry_count {
                                if buf.remaining() < entry_len {
                                    break;
                                }
                                let entry_idx = buf.get_u16();
                                let string_len = entry_len - 2;
                                let raw = buf.copy_to_bytes(string_len);
                                if entry_idx == 0x0001 {
                                    let null_end =
                                        raw.iter().position(|&x| x == 0).unwrap_or(string_len);
                                    let code = String::from_utf8_lossy(&raw[..null_end])
                                        .trim()
                                        .to_string();
                                    if !code.is_empty() {
                                        return Ok(OrderCode { code, v1, v2, v3 });
                                    }
                                }
                            }
                        }

                        // Fallback: scan for "6ES"/"6AV"/"6GK" pattern anywhere in payload.
                        let code = scan_ascii_fields(&payload, 10, 4)
                            .into_iter()
                            .find(|s| {
                                let su = s.to_uppercase();
                                (su.starts_with("6ES")
                                    || su.starts_with("6AV")
                                    || su.starts_with("6GK"))
                                    && s.len() >= 10
                                    && s.bytes().all(|c| c.is_ascii_graphic() || c == b' ')
                            })
                            .unwrap_or_default();
                        Ok(OrderCode { code, v1, v2, v3 })
                    }
                    _ => Err(S7Error::Error("Datum is not Szl".into()))?,
                }
            }
        }
    }

    pub async fn get_cpu_info(&self) -> Result<CpuInfo, S7Error> {
        let resp_data = self.szl_read(0x001C, 0x0000).await?;
        match resp_data.datum {
            None => Err(S7Error::Error("Response has no any data".into()))?,
            Some(d) => {
                match d {
                    Datum::Szl(s) => {
                        let payload = s.all_data.unwrap();
                        let szl_id = s.szl_id;
                        let _szl_index = s.szl_index;
                        let entry_len = s.partlist_byte_len.unwrap() as usize;
                        let entry_count = s.partlist_count.unwrap() as usize;

                        let mut buf = BytesMut::from(payload.as_slice());
                        if szl_id == 0x001C && entry_len >= 4 && entry_count > 0 {
                            let mut module_type = String::new();
                            let mut serial_number = String::new();
                            let mut as_name = String::new();
                            let mut copyright = String::new();
                            let mut module_name = String::new();
                            let mut sn_mc = String::new();
                            for _ in 0..entry_count {
                                if buf.remaining() < entry_len {
                                    break;
                                }
                                let entry_idx = buf.get_u16();
                                let string_len = entry_len - 2;
                                let raw = buf.copy_to_bytes(string_len);
                                let null_end =
                                    raw.iter().position(|&x| x == 0).unwrap_or(string_len);
                                let val =
                                    String::from_utf8_lossy(&raw[..null_end]).trim().to_string();

                                match entry_idx {
                                    0x0001 => {
                                        if as_name.is_empty() {
                                            as_name = val;
                                        }
                                    }
                                    // 0x0002 is module type on S7-300, AS name on S7-1500 — only use if
                                    // 0x0007 is absent (module_type_canonical will override below).
                                    0x0002 => {
                                        if module_name.is_empty() {
                                            module_name = val;
                                        }
                                    }
                                    0x0003 => {
                                        if module_name.is_empty() {
                                            module_name = val;
                                        }
                                    }
                                    0x0004 => {
                                        if copyright.is_empty() {
                                            copyright = val;
                                        }
                                    }
                                    0x0005 => {
                                        if serial_number.is_empty() {
                                            serial_number = val;
                                        }
                                    }
                                    // 0x0007 is always the true module type name (both S7-300 and S7-1500)
                                    0x0007 => {
                                        if module_type.is_empty() {
                                            module_type = val;
                                        }
                                    }
                                    // 0x0008 is SMC memory card on S7-1500 — do not use for module_name
                                    0x0008 => {
                                        if sn_mc.is_empty() {
                                            sn_mc = val;
                                        }
                                    }
                                    _ => {}
                                }
                            }

                            if module_name.is_empty() && !as_name.is_empty() {
                                module_name = as_name.clone();
                            }

                            if !module_type.is_empty()
                                || !serial_number.is_empty()
                                || !as_name.is_empty()
                            {
                                return Ok(CpuInfo {
                                    module_type,
                                    serial_number,
                                    as_name,
                                    copyright,
                                    module_name,
                                });
                            }
                        }

                        // S7-1500 and some firmware variants use a tagged sub-record format.
                        // Fall back to scanning the raw payload for tagged string fields.
                        let data = payload.as_ref();
                        let (module_type, serial_number, as_name, copyright, module_name) =
                            parse_sub_record_fields(data);

                        if !module_type.is_empty() || !serial_number.is_empty() {
                            return Ok(CpuInfo {
                                module_type,
                                serial_number,
                                as_name,
                                copyright,
                                module_name,
                            });
                        }

                        // Last-resort scan: extract printable strings and apply heuristics.
                        let mut module_type = String::new();
                        let mut serial_number = String::new();
                        let mut as_name = String::new();
                        let mut copyright = String::new();
                        let mut module_name = String::new();

                        let mut scan = 0;
                        while scan < data.len() {
                            if data[scan].is_ascii_graphic() || data[scan] == b' ' {
                                let start = scan;
                                while scan < data.len()
                                    && (data[scan].is_ascii_graphic() || data[scan] == b' ')
                                {
                                    scan += 1;
                                }
                                let val = String::from_utf8_lossy(&data[start..scan])
                                    .trim()
                                    .to_string();
                                if val.len() >= 3 {
                                    let tag = if start >= 2 && data[start - 2] == 0x00 {
                                        Some(data[start - 1])
                                    } else {
                                        None
                                    };
                                    let su = val.to_uppercase();
                                    if su.contains("BOOT")
                                        || su.starts_with("P B")
                                        || su.starts_with("HBOOT")
                                    {
                                        // skip firmware label
                                    } else if tag == Some(0x07) && module_type.is_empty() {
                                        module_type = val;
                                    } else if tag == Some(0x08) && module_name.is_empty() {
                                        module_name = val;
                                    } else if tag == Some(0x05) && as_name.is_empty() {
                                        as_name = val;
                                    } else if tag == Some(0x06) && copyright.is_empty() {
                                        copyright = val;
                                    } else if tag == Some(0x04) && serial_number.is_empty() {
                                        serial_number = val;
                                    } else if val.contains('-')
                                        && val.chars().filter(|c| c.is_ascii_digit()).count() >= 4
                                        && !val.starts_with("6ES7")
                                        && serial_number.is_empty()
                                    {
                                        serial_number = val;
                                    } else if su.contains("CPU")
                                        && su.contains("PN")
                                        && module_type.is_empty()
                                    {
                                        module_type = val;
                                    } else if module_type.is_empty()
                                        && val.len() >= 8
                                        && !su.contains("MC_")
                                    {
                                        module_type = val;
                                    }
                                }
                            } else {
                                scan += 1;
                            }
                        }

                        Ok(CpuInfo {
                            module_type,
                            serial_number,
                            as_name,
                            copyright,
                            module_name,
                        })
                    }
                    _ => Err(S7Error::Error("Datum is not Szl".into()))?,
                }
            }
        }
    }

    pub async fn get_communication_info(&self) -> Result<CommunicationInfo, S7Error> {
        let resp_data = self.szl_read(0x0131, 0x0001).await?;
        match resp_data.datum {
            None => Err(S7Error::Error("Response has no any data".into()))?,
            Some(d) => {
                match d {
                    Datum::Szl(s) => {
                        let payload = s.all_data.unwrap();

                        let szl_id = s.szl_id;
                        let _szl_index = s.szl_index;
                        let entry_len = s.partlist_byte_len.unwrap() as usize;
                        let entry_count = s.partlist_count.unwrap() as usize;

                        let mut buf = BytesMut::from(payload.as_slice());
                        if szl_id == 0x0131
                            && entry_len >= 12
                            && entry_count >= 1
                            && buf.remaining() >= entry_len
                        {
                            let _entry_idx = buf.get_u16();
                            let max_pdu_len = buf.get_u16() as u32;
                            let max_connections = buf.get_u16() as u32;
                            let max_mpi_rate = buf.get_u32();
                            let max_bus_rate = buf.get_u32();
                            return Ok(CommunicationInfo {
                                max_pdu_len,
                                max_connections,
                                max_mpi_rate,
                                max_bus_rate,
                            });
                        }

                        // Fallback: scan for any parseable numeric data
                        Ok(CommunicationInfo {
                            max_pdu_len: 0,
                            max_connections: 0,
                            max_mpi_rate: 0,
                            max_bus_rate: 0,
                        })
                    }
                    _ => Err(S7Error::Error("Datum is not Szl".into()))?,
                }
            }
        }
    }

    pub async fn get_plc_status(&self) -> Result<PlcStatus, S7Error> {
        let resp_data = self.szl_read(0x0424, 0x0000).await?;
        match resp_data.datum {
            None => Err(S7Error::Error("Response has no any data".into()))?,
            Some(d) => {
                match d {
                    Datum::Szl(s) => {
                        let payload = s.all_data.unwrap();

                        let szl_id = s.szl_id;
                        let _szl_index = s.szl_index;

                        if szl_id == 0x0424 {
                            // payload[0..1]: ereig
                            // payload[2]: ae
                            // payload[3]: bzu-id requested model: Run, Stop ...
                            let status_byte = payload[3];
                            return match status_byte {
                                0x00 => Ok(PlcStatus::Unknown),
                                0x04 => Ok(PlcStatus::Stop),
                                0x08 => Ok(PlcStatus::Run),
                                // Old CPUs sometimes encode STOP as 0x03
                                0x03 => Ok(PlcStatus::Stop),
                                _ => Ok(PlcStatus::Stop),
                            };
                        }
                        // Fallback: scan for any parseable numeric data
                        Ok(PlcStatus::Unknown)
                    }
                    _ => Err(S7Error::Error("Datum is not Szl".into()))?,
                }
            }
        }
    }
}

fn scan_ascii_fields(data: &[u8], max_count: usize, min_len: usize) -> Vec<String> {
    let mut fields = Vec::new();
    let mut i = 0;
    while i < data.len() && fields.len() < max_count {
        // Skip bytes that are not visible ASCII (0x20-0x7E)
        if !data[i].is_ascii_graphic() && data[i] != b' ' {
            i += 1;
            continue;
        }
        // Collect a run of visible ASCII
        let start = i;
        while i < data.len() && (data[i].is_ascii_graphic() || data[i] == b' ') {
            i += 1;
        }
        let s = String::from_utf8_lossy(&data[start..i]).trim().to_string();
        if s.len() >= min_len {
            fields.push(s);
        }
    }
    fields
}

/// Parse the S7-300 sub-record format used in SZL 0x001C responses.
///
/// This format uses tagged records: `[00 <tag> <string>] ...` where
/// known tags are:
/// - 0x01: order code / module identification
/// - 0x05: plant identification (AS name)
/// - 0x06: serial number
/// - 0x07: module type name
/// - 0x08: module name
fn parse_sub_record_fields(b: &[u8]) -> (String, String, String, String, String) {
    let mut module_type = String::new();
    let mut serial_number = String::new();
    let mut as_name = String::new();
    let mut copyright = String::new();
    let mut module_name = String::new();

    let mut i = 0;
    while i + 2 < b.len() {
        // Look for 00 <tag> pattern with a known sub-record tag (1..=8)
        if b[i] == 0x00 && (1..=8).contains(&b[i + 1]) {
            let tag = b[i + 1];
            let start = i + 2;

            // Find end of string: next 0x00 byte (including 00 C0)
            let mut end = start;
            while end < b.len() && b[end] != 0x00 {
                end += 1;
            }

            let raw = &b[start..end];
            let val = String::from_utf8_lossy(raw).trim().to_string();

            // Skip empty and firmware-label values
            let su = val.to_uppercase();
            if !val.is_empty() && !su.contains("BOOT") && !su.starts_with("P B") {
                match tag {
                    0x01 => {
                        // Tag 0x01 may be order code (starts with "6ES") or module type.
                        if !val.starts_with("6ES") && module_type.is_empty() {
                            module_type = val;
                        }
                    }
                    0x05 => {
                        if as_name.is_empty() {
                            as_name = val;
                        }
                    }
                    0x06 => {
                        if serial_number.is_empty() {
                            serial_number = val;
                        }
                    }
                    0x07 => {
                        if module_type.is_empty() {
                            module_type = val;
                        }
                    }
                    0x08 => {
                        if module_name.is_empty() {
                            module_name = val;
                        }
                    }
                    _ => {}
                }
            }

            i = end;
        } else {
            i += 1;
        }
    }

    // Also scan for free-standing printable strings that look like copyright
    // (e.g. "Boot Loader" appearing after the tagged records).
    if copyright.is_empty() {
        let mut scan = 0;
        while scan < b.len() {
            if b[scan].is_ascii_graphic() || b[scan] == b' ' {
                let s = scan;
                while scan < b.len() && (b[scan].is_ascii_graphic() || b[scan] == b' ') {
                    scan += 1;
                }
                let val = String::from_utf8_lossy(&b[s..scan]).trim().to_string();
                let su = val.to_uppercase();
                if val.len() >= 3 {
                    if su.contains("BOOT") || su.starts_with("P B") {
                        copyright = val;
                        break;
                    }
                }
            } else {
                scan += 1;
            }
        }
    }

    (module_type, serial_number, as_name, copyright, module_name)
}

/// 将 Rust 字符串转换为 Latin-1 (ISO-8859-1) 字节序列。
/// 若包含无法编码的字符则返回错误。
fn str_to_latin1(s: &str) -> Result<Vec<u8>, S7Error> {
    let mut bytes = Vec::with_capacity(s.len());
    for c in s.chars() {
        if c as u32 > 0xFF {
            return Err(S7Error::Error(format!(
                "Character '{}' cannot be encoded in Latin-1 / STRING",
                c
            )));
        }
        bytes.push(c as u8);
    }
    Ok(bytes)
}

/// 计算 COTP 连接请求的 `(source_tsap, destination_tsap)`。
///
/// source TSAP 首字节是连接类型（PG=0x01 / OP=0x02 / S7=0x03），次字节 0x00。
/// 旧实现恒为 `0x0100`，使 [`S7Config::connection_type`] 成为死配置（S-9）。
/// S7-200 / 200Smart / Sinumerik828D 用厂商固定的 TSAP，不参与该编码。
fn tsaps(config: &S7Config) -> (u16, u16) {
    match config.plc_type {
        PlcType::S200 => (0x4D57, 0x4D57),
        PlcType::S200Smart => (0x1000, 0x3000),
        PlcType::Sinumerik828D => (0x0400, 0x0D04),
        _ => (
            (config.connection_type as u16) << 8,
            0x0300 + (0x20 * config.rack + config.slot) as u16,
        ),
    }
}

async fn run_actor(
    config: S7Config,
    initial_pdu: u16,
    mut cmd_rx: mpsc::Receiver<Command>,
    result_tx: oneshot::Sender<Result<u16, S7Error>>, // 原始参数
) {
    // 包装为 Option，方便安全取出
    let mut result_tx = Some(result_tx);
    let mut pdu_length = initial_pdu;

    loop {
        // ---------- 1. 建立 TCP 连接 ----------
        let addr: SocketAddr = match format!("{}:{}", config.host, config.port).parse() {
            Ok(a) => a,
            Err(e) => {
                // 如果首次连接，必须报告错误
                if let Some(tx) = result_tx.take() {
                    let _ = tx.send(Err(S7Error::Error(format!("Invalid address: {}", e))));
                    return;
                }
                // 重连时地址错误则等待重试
                if !config.auto_reconnect {
                    return; // 禁止重连，结束
                }
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        let mut stream = match tokio::time::timeout(
            Duration::from_millis(config.timeout_ms),
            TcpStream::connect(&addr),
        )
        .await
        {
            Ok(Ok(s)) => s,
            _ => {
                if let Some(tx) = result_tx.take() {
                    let _ = tx.send(Err(S7Error::ConnectionRefused {
                        host: config.host.clone(),
                        port: config.port,
                    }));
                    return;
                }
                warn!("Reconnect failed, retrying...");
                if !config.auto_reconnect {
                    return; // 禁止重连，结束
                }
                sleep(Duration::from_secs(2)).await;
                continue;
            }
        };

        let (local, remote) = tsaps(&config);

        // ---------- 2. 握手 ----------
        let handshake_result = handshake(&mut stream, local, remote, pdu_length).await;

        // ---------- 3. 处理结果（首次连接 vs 重连）----------
        if let Some(tx) = result_tx.take() {
            // 首次连接：必须通知调用者
            match handshake_result {
                Ok(p) => {
                    pdu_length = p;
                    let _ = tx.send(Ok(p));
                }
                Err(e) => {
                    let _ = tx.send(Err(e));
                    return; // 首次失败，终止 actor
                }
            }
        } else {
            // 重连
            match handshake_result {
                Ok(p) => {
                    pdu_length = p;
                    info!("Reconnected, PDU length: {}", p);
                }
                Err(e) => {
                    warn!("Reconnect handshake failed: {}", e);
                    if !config.auto_reconnect {
                        return; // 禁止重连，结束
                    }
                    sleep(Duration::from_secs(2)).await;
                    continue;
                }
            }
        }

        // ---------- 4. 进入命令处理循环 ----------
        match serve_commands(stream, pdu_length, local, remote, &mut cmd_rx).await {
            Ok(r) => {
                if r == 0 {
                    // 主动断开连接
                    break;
                }
            }
            Err(e) => {
                info!("Connection lost: {}, reconnecting...", e);
                if !config.auto_reconnect {
                    return; // 禁止重连，结束
                }
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
}

async fn serve_commands(
    mut stream: TcpStream,
    pdu_limit: u16,
    local: u16,
    remote: u16,
    cmd_rx: &mut mpsc::Receiver<Command>,
) -> Result<u8, S7Error> {
    let next_pdu_ref = AtomicU16::new(0);
    while let Some(cmd) = cmd_rx.recv().await {
        match cmd {
            Command::Read { mut items, reply } => {
                let res = handle_read(&mut stream, pdu_limit, &next_pdu_ref, &mut items).await;
                let _ = reply.send(res.clone());
                if res.is_err() {
                    return Err(res.unwrap_err());
                }
            }
            Command::Write {
                mut items,
                data,
                reply,
            } => {
                let res =
                    handle_write(&mut stream, pdu_limit, &next_pdu_ref, &mut items, data).await;
                let _ = reply.send(res.clone());
                if res.is_err() {
                    return Err(res.unwrap_err());
                }
            }
            Command::ReadSzl {
                szl_id,
                szl_index,
                reply,
            } => {
                let res =
                    handle_read_szl(&mut stream, pdu_limit, &next_pdu_ref, szl_id, szl_index).await;
                let _ = reply.send(res.clone());
                if res.is_err() {
                    return Err(res.unwrap_err());
                }
            }
            Command::Disconnect { reply } => {
                let mut req = S7Data::create_cr_disconnection(local, remote);
                if let Some(h) = &mut req.header {
                    h.pdu_reference = next_pdu_ref.fetch_add(1, Ordering::Relaxed);
                }
                let bytes = req.to_be_bytes();
                if bytes.len() - 7 > pdu_limit as usize {
                    return Err(S7Error::Error("Write request exceeds PDU limit".into()));
                }
                let mut framed = Framed::new(&mut stream, S7DataCodec);
                framed.send(bytes).await?;
                // 优雅关闭 TCP 流
                if let Err(e) = stream.shutdown().await {
                    warn!("Error shutting down TCP: {}", e);
                }
                // 通知调用者断开成功
                let _ = reply.send(Ok(()));
                // 正常返回，不再重连
                return Ok(0);
            }
        }
    }
    Ok(1)
}

// ---------- 读取处理 ----------

async fn handle_read(
    stream: &mut TcpStream,
    pdu_limit: u16,
    next_ref: &AtomicU16,
    items: &mut Vec<RequestItem>,
) -> Result<Vec<DataItem>, S7Error> {
    // 构造请求
    let mut req = S7Data::create_read_request(items);
    if let Some(h) = &mut req.header {
        h.pdu_reference = next_ref.fetch_add(1, Ordering::Relaxed);
    }

    let mut framed = Framed::new(stream, S7DataCodec);

    let bytes = req.to_be_bytes();
    // 检查 PDU 长度（不含 TPKT/COTP 头）
    if bytes.len() - 7 > pdu_limit as usize {
        return Err(S7Error::Error("Read request exceeds PDU limit".into()));
    }

    framed.send(bytes).await?;
    match framed.next().await {
        None => Err(S7Error::Error("No data in read response".into())),
        Some(res) => {
            let raw = res?;
            let resp = S7Data::from_be_bytes(&raw)
                .map_err(|e| S7Error::Error(format!("Parse S7Data: {}", e)))?
                .ok_or_else(|| S7Error::Error("Not a valid S7 data".to_string()))?;
            check_response(&resp)?;

            // 提取数据
            if let Some(Datum::ReadWrite(datum)) = &resp.datum {
                let mut data_items = Vec::new();
                for item in &datum.return_items {
                    if let crate::item::S7ReturnItem::Data(di) = item {
                        data_items.push(di.clone());
                    } else {
                        warn!("Unexpected non-data return item in read response");
                    }
                }
                Ok(data_items)
            } else {
                Err(S7Error::Error("No data in read response".into()))
            }
        }
    }
}

// ---------- 写入处理 ----------
async fn handle_write(
    stream: &mut TcpStream,
    pdu_limit: u16,
    next_ref: &AtomicU16,
    items: &mut Vec<RequestItem>,
    data: Vec<DataItem>,
) -> Result<(), S7Error> {
    // S-1：`create_write_request` 会把 items 移入请求参数，先留存副本，
    // 写应答逐项校验失败时才能回报是哪一项（RequestItem: Copy，仅一次内存拷贝）。
    let req_items = items.clone();
    let mut req = S7Data::create_write_request(items, data);
    if let Some(h) = &mut req.header {
        h.pdu_reference = next_ref.fetch_add(1, Ordering::Relaxed);
    }

    let bytes = req.to_be_bytes();
    if bytes.len() - 7 > pdu_limit as usize {
        return Err(S7Error::Error("Write request exceeds PDU limit".into()));
    }

    let mut framed = Framed::new(stream, S7DataCodec);
    framed.send(bytes).await?;
    match framed.next().await {
        None => Err(S7Error::Error("No data in write response".into())),
        Some(res) => {
            let raw = res?;
            let resp = S7Data::from_be_bytes(&raw)
                .map_err(|e| S7Error::Error(format!("Parse S7Data: {}", e)))?
                .ok_or_else(|| S7Error::Error("Not a valid S7 data".to_string()))?;
            check_response(&resp)?;
            check_write_return_items(&resp, &req_items)?;
            Ok(())
        }
    }
}

async fn handle_read_szl(
    stream: &mut TcpStream,
    pdu_limit: u16,
    next_ref: &AtomicU16,
    szl_id: u16,
    szl_index: u16,
) -> Result<S7Data, S7Error> {
    // 构造请求
    let mut req = S7Data::create_szl_request(szl_id, szl_index);
    if let Some(h) = &mut req.header {
        h.pdu_reference = next_ref.fetch_add(1, Ordering::Relaxed);
    }

    let bytes = req.to_be_bytes();
    // 检查 PDU 长度（不含 TPKT/COTP 头）
    if bytes.len() - 7 > pdu_limit as usize {
        return Err(S7Error::Error("Read request exceeds PDU limit".into()));
    }

    let mut framed = Framed::new(stream, S7DataCodec);
    framed.send(bytes).await?;

    if let Some(Ok(raw)) = framed.next().await {
        let resp = S7Data::from_be_bytes(&raw)
            .map_err(|e| S7Error::Error(format!("Parse S7Data: {}", e)))?
            .ok_or_else(|| S7Error::Error("Not a valid S7 data".to_string()))?;
        check_response(&resp)?;
        Ok(resp)
    } else {
        Err(S7Error::Error("No data in read response".into()))
    }
}

// ---------- 响应校验 ----------

fn check_response(resp: &S7Data) -> Result<(), S7Error> {
    if let Some(h) = &resp.header {
        if h.error_class.is_some()
            && h.error_code.is_some()
            && h.error_class.unwrap() != S7ErrorClass::NoError
        {
            return Err(S7Error::Error(format!(
                "Response error: class={:?}, code={}",
                h.error_class,
                h.error_code.unwrap()
            )));
        }
    }
    Ok(())
}

/// S-1：写应答的 item 级返回码校验。
///
/// S7 写应答（AckData + WriteVariable）的 datum 是每个请求项一个 `ReturnItem`（1 字节），
/// 与请求项**同序**。此前只做头级 [`check_response`]，item 级 `return_code` 被整段丢弃
/// （PLC 回 `0x05 InvalidAddress` 也当成写成功）。任一项非 `Success` 即返回错误，
/// 错误里带 item 序号、请求地址与返回码。
fn check_write_return_items(resp: &S7Data, req_items: &[RequestItem]) -> Result<(), S7Error> {
    let Some(Datum::ReadWrite(datum)) = &resp.datum else {
        // 无 datum 的应答（异常形态）由 check_response / 上层处理
        return Ok(());
    };
    for (idx, item) in datum.return_items.iter().enumerate() {
        let S7ReturnItem::Return(ret) = item else {
            continue;
        };
        if ret.return_code != S7ReturnCode::Success {
            let addr = req_items
                .get(idx)
                .map(format_request_item)
                .unwrap_or_else(|| "<unknown>".to_string());
            return Err(S7Error::ProtocolError {
                code: ret.return_code.code() as u32,
                message: format!(
                    "write item {} ({}) rejected: {}",
                    idx,
                    addr,
                    ret.return_code.description()
                ),
            });
        }
    }
    Ok(())
}

/// 诊断用：把请求项还原成 S7 地址文本（`DB1.DBX0.0` / `M10.0`）。
fn format_request_item(item: &RequestItem) -> String {
    match item.area {
        S7Area::DataBlocks | S7Area::InstanceDataBlocks => format!(
            "{}{}.DBX{}.{}",
            item.area.abbr(),
            item.db_number,
            item.byte_address,
            item.bit_address
        ),
        _ => format!(
            "{}{}.{}",
            item.area.abbr(),
            item.byte_address,
            item.bit_address
        ),
    }
}

async fn handshake(
    stream: &mut TcpStream,
    local: u16,
    remote: u16,
    pdu_length: u16,
) -> Result<u16, S7Error> {
    // ---------- COTP 连接 ----------
    let req = S7Data::create_cr_connection(local, remote);
    let mut framed = Framed::new(stream, S7DataCodec);
    framed.send(req.to_be_bytes()).await?;
    match framed.next().await {
        None => {
            return Err(S7Error::Error(
                "COTP cc error: connection closed".to_string(),
            ));
        }
        Some(res) => {
            let raw = res?;
            let cotp_resp = S7Data::from_be_bytes(&raw)
                .map_err(|e| S7Error::Error(format!("Parse S7Data: {}", e)))?
                .ok_or_else(|| S7Error::Error("Not a valid S7 data".to_string()))?;
            check_response(&cotp_resp)?;

            let cotp = cotp_resp
                .cotp
                .as_ref()
                .ok_or_else(|| S7Error::Error("Missing COTP in response".to_string()))?;
            if cotp.pdu_type() != PduType::ConnectConfirm {
                error!("COTP 连接被拒绝: {:?}", cotp.pdu_type());
                return Err(S7Error::Error("COTP connection refused".to_string()));
            }

            info!("COTP Connection Confirmed");
        }
    }

    // ---------- S7 Setup 协商 ----------
    let req = S7Data::create_setup_comm(pdu_length);
    framed.send(req.to_be_bytes()).await?;

    match framed.next().await {
        None => Err(S7Error::Error("setup error: connection closed".to_string())),
        Some(res) => {
            let raw = res?;
            let setup_resp = S7Data::from_be_bytes(&raw)
                .map_err(|e| S7Error::Error(format!("Parse S7Data: {}", e)))?
                .ok_or_else(|| S7Error::Error("Not a valid S7 data".to_string()))?;
            check_response(&setup_resp)?;

            // 检查 COTP 类型
            let setup_cotp = setup_resp
                .cotp
                .as_ref()
                .ok_or_else(|| S7Error::Error("Missing COTP in setup response".to_string()))?;
            if setup_cotp.pdu_type() != PduType::DtData {
                error!("Setup 响应 COTP 类型错误: {:?}", setup_cotp.pdu_type());
                return Err(S7Error::Error("Setup response COTP type error".to_string()));
            }

            // 检查头部错误
            if let Some(h) = &setup_resp.header {
                if h.error_class.is_some()
                    && h.error_code.is_some()
                    && h.error_class.unwrap() != S7ErrorClass::NoError
                {
                    error!(
                        "Setup 响应错误: {:?} code={}",
                        h.error_class,
                        h.error_code.unwrap()
                    );
                    return Err(S7Error::Error(format!(
                        "Setup failed: class={:?}, code={}",
                        h.error_class,
                        h.error_code.unwrap()
                    )));
                }
            } else {
                error!("Setup 响应Header 缺失");
                return Err(S7Error::Error(
                    "Missing header in setup response".to_string(),
                ));
            }

            // 提取协商后的 PDU 长度
            let negotiated_pdu =
                match setup_resp.parameter.as_ref().ok_or_else(|| {
                    S7Error::Error("Missing parameter in setup response".to_string())
                })? {
                    S7Parameter::SetupParameter(s) => {
                        info!("S7 Setup complete，negotiated PDU length: {}", s.pdu_length);
                        s.pdu_length
                    }
                    _ => {
                        error!("Setup 响应参数类型不匹配");
                        return Err(S7Error::Error("Setup parameter type mismatch".to_string()));
                    }
                };

            Ok(negotiated_pdu)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{S7DataVariableType, S7FunctionCode, S7MessageType};
    use tokio::io::AsyncReadExt;
    use tokio::net::TcpListener;

    /// 起一个一次性 mock PLC：读完 client 发来的一帧请求（TPKT 头 4 字节 + 剩余），
    /// 回一段预先编好的应答字节，然后保持连接直到 client 关闭。
    async fn spawn_mock_plc(response: Vec<u8>) -> TcpStream {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut head = [0u8; 4];
            sock.read_exact(&mut head).await.unwrap();
            let total = u16::from_be_bytes([head[2], head[3]]) as usize;
            let mut body = vec![0u8; total.saturating_sub(4)];
            sock.read_exact(&mut body).await.unwrap();
            sock.write_all(&response).await.unwrap();
            sock.flush().await.unwrap();
            let mut sink = Vec::new();
            let _ = sock.read_to_end(&mut sink).await;
        });
        TcpStream::connect(addr).await.unwrap()
    }

    /// 构造写应答帧：TPKT + COTP DT + AckData 头（带 2 字节错误区）+ WriteVariable 参数 +
    /// 每项 1 字节 return_code 的 datum。
    fn write_response(return_codes: &[u8]) -> Vec<u8> {
        let param_len: usize = 2;
        let data_len = return_codes.len();
        let total = 4 + 3 + 12 + param_len + data_len;
        let mut f = Vec::with_capacity(total);
        f.extend_from_slice(&[0x03, 0x00]);
        f.extend_from_slice(&(total as u16).to_be_bytes());
        f.extend_from_slice(&[0x02, 0xF0, 0x80]); // COTP DT，last_data_unit=1
        f.push(0x32); // protocol id
        f.push(S7MessageType::AckData.code());
        f.extend_from_slice(&[0x00, 0x00]); // reserved
        f.extend_from_slice(&0x0001u16.to_be_bytes()); // pdu reference
        f.extend_from_slice(&(param_len as u16).to_be_bytes());
        f.extend_from_slice(&(data_len as u16).to_be_bytes());
        f.extend_from_slice(&[0x00, 0x00]); // error class / error code
        f.push(S7FunctionCode::WriteVariable.code());
        f.push(return_codes.len() as u8);
        f.extend_from_slice(return_codes);
        f
    }

    #[tokio::test]
    async fn s1_write_success_return_code_is_ok() {
        let mut stream = spawn_mock_plc(write_response(&[0xFF])).await;
        let mut items = vec![RequestItem::parse_byte("DB1.DBB0", 1).unwrap()];
        let data = vec![DataItem::create_req_bytes(vec![0x01]).unwrap()];
        let next_ref = AtomicU16::new(0);
        handle_write(&mut stream, 960, &next_ref, &mut items, data)
            .await
            .expect("return_code=0xFF(Success) 应写成功");
    }

    #[tokio::test]
    async fn s1_write_fails_on_item_level_error() {
        // 第 0 项 Success、第 1 项 0x05 InvalidAddress：必须报错并指出是哪一项。
        let mut stream = spawn_mock_plc(write_response(&[0xFF, 0x05])).await;
        let mut items = vec![
            RequestItem::parse_byte("DB1.DBB0", 1).unwrap(),
            RequestItem::parse_byte("DB2.DBB0", 1).unwrap(),
        ];
        let data = vec![
            DataItem::create_req_bytes(vec![0x01]).unwrap(),
            DataItem::create_req_bytes(vec![0x02]).unwrap(),
        ];
        let next_ref = AtomicU16::new(0);
        let err = handle_write(&mut stream, 960, &next_ref, &mut items, data)
            .await
            .expect_err("return_code=0x05(InvalidAddress) 不能再当成写成功");
        match err {
            S7Error::ProtocolError { code, message } => {
                assert_eq!(code, 0x05);
                assert!(message.contains("item 1"), "应指出失败项序号: {message}");
                assert!(message.contains("DB2.DBX0.0"), "应指出失败项地址: {message}");
                assert!(message.contains("invalid address"), "应含返回码描述: {message}");
            }
            other => panic!("期望 ProtocolError，实际 {other:?}"),
        }
    }

    /// S-5：STRING 读按 Latin-1（ISO-8859-1）解码，0x80-0xFF 逐字节映射到同码位字符，
    /// 与写侧 `str_to_latin1` 对称（旧实现 from_utf8_lossy → U+FFFD 替换字符）。
    #[tokio::test]
    async fn s5_read_string_decodes_latin1() {
        let (tx, mut rx) = mpsc::channel(4);
        // 桩 actor：应答 STRING 数据区 [max_len=0x0A, actual_len=0x02, 0xB0, 0x43]
        tokio::spawn(async move {
            if let Some(Command::Read { reply, .. }) = rx.recv().await {
                let item = DataItem {
                    return_code: S7ReturnCode::Success,
                    data_variable_type: S7DataVariableType::ByteWordDword,
                    count: 4,
                    data: vec![0x0A, 0x02, 0xB0, 0x43],
                };
                let _ = reply.send(Ok(vec![item]));
            }
        });
        let mut client = S7Client::new(S7Config::default());
        client.cmd_tx = Some(tx);

        let s = client.read_string("DB1.DBB0", 10).await.unwrap();
        assert_eq!(s, "°C");
        assert_eq!(s.as_bytes(), &[0xC2, 0xB0, 0x43], "应为 UTF-8 编码的 °C");
        // 读写对称：写侧编码回去必须是原字节
        assert_eq!(str_to_latin1(&s).unwrap(), vec![0xB0, 0x43]);
    }

    /// S-9：connection_type（PG/OP/S7）编入 local TSAP 首字节。
    #[test]
    fn s9_connection_type_lands_in_local_tsap() {
        let pg = tsaps(&S7Config::default().with_connection_type(0x01));
        let op = tsaps(&S7Config::default().with_connection_type(0x02));
        let s7 = tsaps(&S7Config::default().with_connection_type(0x03));
        // 线上是大端：首字节 = 连接类型，次字节 = 0x00
        assert_eq!(pg.0.to_be_bytes(), [0x01, 0x00]);
        assert_eq!(op.0.to_be_bytes(), [0x02, 0x00]);
        assert_eq!(s7.0.to_be_bytes(), [0x03, 0x00]);
        assert_ne!(pg.0, op.0);
        assert_ne!(op.0, s7.0);
        // remote TSAP（机架/槽位）不受连接类型影响：S1200 rack0 slot2
        assert_eq!(pg.1, op.1);
        assert_eq!(pg.1, 0x0300 + 2);
        // 厂商固定 TSAP 的机型不被 connection_type 覆盖
        assert_eq!(
            tsaps(&S7Config::default()
                .with_plc_type(PlcType::S200)
                .with_connection_type(0x02))
            .0,
            0x4D57
        );
        assert_eq!(
            tsaps(&S7Config::default()
                .with_plc_type(PlcType::S200Smart)
                .with_connection_type(0x02))
            .0,
            0x1000
        );
    }
}
