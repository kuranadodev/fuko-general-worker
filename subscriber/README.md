# Subscriber 设计方案（子系统）

## 目标
- 子系统独立部署（Flask/FastAPI 进程可集成消费者线程）。
- 可动态订阅/取消订阅 routing key。
- 不依赖主系统代码变更。

## 关键设计
1. 子系统各自创建临时队列（exclusive + auto-delete）。
2. 队列策略：
   - `x-message-ttl=60000`
   - `x-expires=300000`
   - `x-max-length=1000`
   - `overflow=drop-head`
3. 收到事件后根据 `file_id` 拉 Mongo 数据并执行业务分析。
4. 使用幂等键建议：`file_id + subsystem_id + algorithm_version`。

## 工作流
1. 子系统启动并声明 exchange 与临时队列。
2. 绑定配置中的 routing key。
3. 消费事件并执行业务分析。
4. 写回分析结果与处理状态。

## 文件说明
- `main.py`: Subscriber 核心实现与处理占位逻辑。
- `test.py`: 调用 demo，演示订阅启动方式。
