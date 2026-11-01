# UPSTREAM.md — tokio-s7 fork（rs-iot 补丁分支）

本仓是 **tokio-s7 0.1.7 的最小 fork**，只含 rs-iot 协议语义台账中"必须改库内部"的 4 个补丁
（S-1 / S-4 / S-5 / S-9），无格式化、无重构、无 API 变更。

- 问题台账：`rs-iot/docs/designs/2026-11-01-protocol-semantics-audit.md` §5
- fork 裁决：`rs-iot/docs/decisions/2026-11-01-upstream-library-fork-policy.md` §3/§4

## 1. 上游基线

| 项 | 值 |
|---|---|
| crate | `tokio-s7` |
| 版本 | **`0.1.7`（必须保持，不要改）**——rs-iot 依赖声明是 `version = "0.1"`，改版本号会让 `[patch.crates-io]` 匹配不上 |
| 上游仓 | <https://github.com/CrayfishGo/tokio-s7> |
| 上游 commit | `c7c743cc1954a1227c19c5e88b98b5fce5fbff0c`（取自发布包 `.cargo_vcs_info.json`） |
| 源码来源 | cargo registry 发布包（本机 rsproxy.cn 镜像）`tokio-s7-0.1.7`，非 git clone |
| 本仓基线 commit | `03a1d7e` `chore: tokio-s7 0.1.7 upstream baseline` |
| 许可 | Apache-2.0。`LICENSE` 原样保留；被修改文件保留原有版权与文件头声明 |

## 2. 补丁集（每个补丁一个 commit，附库内单测）

| 台账 ID | commit | 位置 | 改动 | 新增单测 |
|---|---|---|---|---|
| **S-1**（P0） | `e2bb3aa` | `src/client.rs:1379`（`handle_write`）、`:1400`（调用点）、`:1463` `check_write_return_items`、`:1492` `format_request_item` | 写应答逐项校验 `ReturnItem.return_code`，非 `Success` → `S7Error::ProtocolError{code, message}`（message 含 item 序号、请求地址、返回码描述） | `client::tests::s1_write_success_return_code_is_ok`、`s1_write_fails_on_item_level_error`（mock PLC 回 `0xFF` / `0xFF,0x05`） |
| **S-4**（P1） | `fa3c4fc` | `src/header.rs:163-174` | 错误区按两字节 `(error_class, error_code)` 依序读取，去掉同名变量 `ec` 覆盖与多读的第 3 字节 | `header::tests::s4_error_region_is_class_then_code`（`0x85/0x04` 帧）、`s4_no_error_region_parses_as_no_error` |
| **S-5**（P1） | `5c83600` | `src/client.rs:530-533`（`read_string`） | STRING 改 ISO-8859-1 逐字节解码（`byte as char`），不再 `from_utf8_lossy`；与写侧 `str_to_latin1` 对称 | `client::tests::s5_read_string_decodes_latin1`（`0xB0 0x43` → `"°C"`，并断言写侧回编码为原字节） |
| **S-9**（P2） | `9d851c5` | `src/client.rs:1128-1144`（新增 `tsaps`）、`:1198`（调用点） | `connection_type` 编入 local TSAP 首字节（PG `0x0100` / OP `0x0200` / S7 `0x0300`）；remote TSAP 不变 | `client::tests::s9_connection_type_lands_in_local_tsap` |

单测：新增 **6 个测试函数**（S-1 两个、S-4 两个、S-5 一个、S-9 一个），全部 `#[tokio::test]` / `#[test]`，
不依赖真机、网络或外部进程（S-1 用进程内 TCP mock PLC，S-5 用内存命令桩，S-4/S-9 为纯函数）。

## 3. 修 bug 前后的红绿证据（每个补丁都按"先红后绿"验证）

| 补丁 | 去掉修复后的失败输出 |
|---|---|
| S-1 | `return_code=0x05(InvalidAddress) 不能再当成写成功` → 断言失败（旧代码返回 `Ok(())`） |
| S-4 | `Error("Unknown error class: 0x85")`；12 字节裸头场景另触发 `advance out of bounds: the len is 0`（旧代码多读第 3 字节） |
| S-5 | `left: "�C"  right: "°C"` |
| S-9 | `left: [1, 0]  right: [2, 0]`（OP 连接仍用 PG 的 TSAP） |

## 4. 已知行为变化与后果

1. **S-1**：写失败会**真的**返回 `Err`。库内 `serve_commands` 对 `handle_write` 的任何 `Err`
   都会结束当前连接的命令循环（与读失败、头级错误一致），因此"一次写被 PLC 拒绝"之后该连接不可用，
   需重连（`auto_reconnect=true` 时 actor 自重连，间隔 2s）。rs-iot 侧对写失败已有 S-2 的处理计划
   （先 `disconnect()` 再 drop）。**若上游接受更细的语义，后续可把"item 级拒绝"改为不拆连接**——
   本 fork 刻意不做，以保持补丁最小、可上游化。
2. **S-4**：错误区仍固定按 12 字节头解析（`byte_len()` 未变），parameter/datum 偏移不受影响；
   仅 class/code 的取值变正确（此前 class 取的是 code 字节，code 取的是 parameter 首字节）。
3. **S-5**：读回的字符串对 0x80-0xFF 是 Latin-1 字符（如 `°`、`ä`），不再是 U+FFFD；ASCII 不受影响。
4. **S-9**：默认 `connection_type = 0x01`（PG）时线上字节与旧版**完全一致**（local TSAP 仍是 `0x0100`）；
   只有显式配置 `pc`/`op`（rs-iot 插件映射为 `0x02`/`0x03`，见 `s7_client/mod.rs` 的 `connection_type` 解析）
   才改变 TSAP。S7-200 / 200Smart / Sinumerik828D 保留厂商固定 TSAP，不参与该编码。

## 5. 验证记录（2026-11-01）

fork 仓自身：

```
$ cargo test
running 11 tests
... test result: ok. 11 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

rs-iot 联调（临时 `[patch.crates-io]` 指向本仓，验证后已还原）：

```
$ cargo tree -i tokio-s7 --features "history,s7"
tokio-s7 v0.1.7 (/root/myprojects/webhmi/dist/vendor/tokio-s7-fork)

$ cargo test --features "history,s7" plugin::builtin::s7_client
test result: ok. 40 passed; 0 failed; 0 ignored; 0 measured; 774 filtered out

$ cargo test --test s7_e2e --features "history,s7"
test result: ok. 4 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

## 6. 接入 rs-iot（用户批准并创建远程仓之后）

```bash
# 1) 推送到团队 git（远端尚不存在时先建仓）
cd /root/myprojects/webhmi/dist/vendor/tokio-s7-fork
git remote add origin https://git.metme.top/viccom/tokio-s7.git
git branch -m rs-iot/fixes          # 当前在 master，改名后再推
git push -u origin rs-iot/fixes
```

```toml
# 2) rs-iot/Cargo.toml 末尾（依赖声明 tokio-s7 = { version = "0.1", optional = true } 保持不动）
[patch.crates-io]
tokio-s7 = { git = "https://git.metme.top/viccom/tokio-s7.git", branch = "rs-iot/fixes" }
```

回退 = 删掉 `[patch.crates-io]` 段，一行回 crates.io 版本（补丁无 API 变更，插件代码不受影响）。

## 7. 退出条件与巡检

- **退出条件**：上游合入对应补丁（或发布含修复的新版本）→ 删该条补丁；4 条全含 → 删 `[patch.crates-io]`，
  切回 crates.io，本仓归档。
- **每季度 / 每次 `cargo update` 前**：检查上游新版本是否已修复；已修复就删补丁并重跑本仓单测 + rs-iot `s7_e2e`。
- **上游升级**：rebase `rs-iot/fixes` 到新 tag，重跑 `cargo test`（本仓）与 rs-iot 两条联调命令。
- **安全**：`cargo-audit` **不覆盖** path/git 依赖，fork 期间由季度巡检人工跟踪上游安全公告与依赖更新，
  结果记在本文件末尾的巡检表里。

### 巡检记录

| 日期 | 上游最新版 | 已合入的补丁 | 动作 |
|---|---|---|---|
| 2026-11-01 | 0.1.7（crates.io，最近提交 2026-07-23） | 无 | 建立 fork（4 补丁） |

## 8. 补丁文件

`rs-iot-fixes.patch` 是本仓 4 个补丁的合并 mbox（`git format-patch --stdout 03a1d7e..rs-iot/fixes`），
供 review / 贴 issue 使用；它是生成物，已被 `.gitignore` 忽略，随时可重新生成。
