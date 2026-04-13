# Publisher 设计方案（主系统）

## 目标
- 主系统只负责向 `events.topic` 发布事件。
- 不关心下游子系统数量，不逐个发送。
- 采用尽力而为语义：子系统在线则收，离线则忽略。

## 关键设计
1. Exchange 统一使用 `topic` 类型，名称 `events.topic`。
2. 消息采用标准信封（event_id、event_type、trace_id、data）。
3. 发布参数：`delivery_mode=1`、`mandatory=False`，避免历史堆积。
4. routing key 命名：`{domain}.{entity}.{action}.{version}`。

## 工作流
1. 主系统完成 SFTP 拉取与预处理。
2. 数据成功写入 MongoDB 后发布 `*.ready` 事件。
3. 子系统自行订阅并拉取 Mongo 数据执行分析。

## 文件说明
- `main.py`: Publisher 核心实现与示例发布入口。
- `test.py`: 调用 demo，演示如何构造并发布事件。
