# supervisord-rs 生产硬伤清单

> TDD 驱动，逐条修复，修完打勾。

## 已修复（全部完成）

- [x] **`numprocs` 无效** — 配置字段存在但从未展开多实例
- [x] **`startsecs` 语义错误** — spawn 后立即变 RUNNING，应等 startsecs 秒确认存活
- [x] **log task handle 丢弃** — 日志写任务 fire-and-forget，写入错误静默丢失
- [x] **固定 1 秒退避** — crash loop 时每秒重启，持续崩溃会打爆 CPU/IO；改为指数退避（上限 32s）
- [x] **`stop_all` 串行** — N 个程序停止需 N × stopwaitsecs，shutdown 极慢；改为并发 stop
- [x] **无 `EXITED` 状态** — 进程正常退出与手动 stop 都显示 STOPPED，监控无法区分
- [x] **无 `STOPPING` 状态** — stop 调用后立即变 STOPPED，进程还在 kill 等待窗口
- [x] **Socket 无鉴权** — 本机任意用户可 stop/signal 所有进程；新增 `allowed_uids` uid 白名单
