# MVCC 事务提交错误处理方案

## 问题描述

在 `commitTransaction` 阶段出现错误时，可能出现**部分写入（partial write）**的情况。

### commitTransaction 流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                        commitTransaction 流程                            │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│  1. 冲突检测                                                             │
│     ↓ 失败 → 返回冲突错误，事务保持 OPEN                                  │
│  2. 分配 commitTs (lastCommittedTs.incrementAndGet())                    │
│     ↓ 内存操作，不会失败                                                  │
│  3. 写入数据到 Raft 日志 (leader.setData)                                 │
│     ↓ 失败 → 返回错误，事务保持 OPEN，内存缓存保留                         │
│  4. 更新 lastCommittedTs 持久化 (updateLastCommittedTs)                   │
│     ↓ 失败 → ⚠️ 数据已写入 Raft 日志，但 lastCommittedTs 未持久化           │
│  5. 更新事务状态为 CLOSE (leader.setData)                                │
│     ↓ 失败 → ⚠️ 数据已写入，但事务状态仍为 OPEN                            │
│  6. 清理内存缓存                                                          │
│     ↓ 失败 → ⚠️ 节点宕机时内存丢失                                         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 风险场景分析

| 场景 | 失败点 | 当前行为 | 风险等级 |
|------|--------|----------|----------|
| **场景 A** | 步骤 3 失败 | 返回错误，内存保留 | ✅ 低风险 - 客户端可重试或回滚 |
| **场景 B** | 步骤 4 失败 | 数据已写入，lastCommittedTs 未持久化 | ⚠️ 中风险 - 重启后可能分配重复 commitTs |
| **场景 C** | 步骤 5 失败 | 数据已写入，事务状态仍为 OPEN | ⚠️ 中风险 - 恢复机制无法识别已提交事务 |
| **场景 D** | 步骤 6 前宕机 | 内存丢失，事务状态 OPEN | ⚠️ 中风险 - 数据已提交但无法清理 |

## 当前实现的保护机制

### Raft 日志保证原子性

步骤 3 的 `leader.setData()` 通过 **Raft 日志复制**实现，提供以下保证：

1. **日志复制的原子性** - 数据要么复制到多数节点，要么完全不复制
2. **应用状态机的原子性** - `ApplyLogTask` 批量应用日志，要么全部应用，要么全部不应用
3. **失败时的一致性** - 如果步骤 3 失败，数据不会被应用到状态机

### 当前代码的局限性

```java
// 当前实现中，异常处理不够完善
} catch (Exception e) {
    LOGGER.error("MVCC: Exception while committing transaction", e);
    return new DataResponest(StatusCode.SYSTEMEXCEPTION, "Exception: " + e.getMessage());
}
// 问题：
// 1. 异常时没有清理缓存，可能导致内存泄漏
// 2. 异常时没有记录事务状态，恢复机制无法处理
// 3. 步骤 5 失败后没有告警或补偿机制
```

## 改进方案

### 方案一：两阶段提交（推荐）

引入**预提交（PREPARE）**状态，确保提交操作的原子性：

```
commitTransaction 改进流程：

1. 冲突检测
2. 分配 commitTs
3. 写入 PREPARE 记录到 Raft 日志（包含事务 ID、commitTs、数据）
4. 写入数据到 Raft 日志
5. 写入 COMMIT 记录到 Raft 日志（将事务状态改为 CLOSE）
6. 清理内存缓存
```

**恢复机制**：
- 重启后扫描 PREPARE 状态的事务，自动完成提交
- 扫描长时间处于 PREPARE 状态的事务，进行回滚

### 方案二：写前日志（WAL）

在提交前先将所有操作记录到日志：

```java
// 伪代码示例
public DataResponest commitTransaction(String request) {
    // 1. 写入 WAL 日志（原子操作）
    WalEntry wal = new WalEntry(transactionId, pendingRows, pendingDeletes);
    saveLog.saveWal(wal);

    // 2. 应用数据
    DataResponest result = applyData(pendingRows, pendingDeletes);

    // 3. 如果成功，删除 WAL 日志
    if (result.isSuccess()) {
        saveLog.deleteWal(transactionId);
    }

    return result;
}
```

### 方案三：补偿事务

当提交失败时，自动执行补偿操作：

```java
// 伪代码示例
public DataResponest commitTransaction(String request) {
    try {
        // 正常提交流程
        ...
    } catch (Exception e) {
        // 记录需要补偿的事务
        compensationQueue.add(transactionId);

        // 异步执行补偿
        executorService.submit(() -> {
            compensateTransaction(transactionId);
        });

        return error(...);
    }
}
```

## 推荐实施方案

### 已实施方案：Leader 初始化回滚（2026-03-06）

**核心思想**：Leader 选举成功后，扫描所有事务状态，自动回滚 OPEN 状态的事务。

**实现细节**：
1. 在 `LeaderRole.init()` 方法中调用 `rollbackOpenTransactions()`
2. 扫描 RocksDB 中所有事务状态记录
3. 对于状态为 OPEN 的事务，将其状态更新为 ROLLBACK

**关键代码**：
```java
// LeaderRole.java
private void rollbackOpenTransactions() throws RocksDBException {
    LOG.info("开始回滚 OPEN 状态的事务...");

    byte[] startKey = KeyUtil.generateTransactionIdKey(0);
    byte[] endKey = KeyUtil.generateTransactionIdKey(Long.MAX_VALUE);

    List<Row> rows = saveData.scan(startKey, endKey);
    int rollbackCount = 0;

    for (Row row : rows) {
        TransactionInfo info = JSON.parseObject(
            new String(row.getValue()), TransactionInfo.class);

        if (info.getStatus() == Status.OPEN) {
            // 发现 OPEN 状态事务，执行回滚
            info.setStatus(Status.ROLLBACK);
            byte[] valueBytes = JSON.toJSONBytes(info);
            setData(JSON.toJSONString(
                new Command(DataOperationType.INSERT, new Row[]{
                    new Row(KeyUtil.generateTransactionIdKey(info.getTransactionId()), valueBytes)
                })
            ));
            rollbackCount++;
        }
    }

    LOG.info("完成 OPEN 状态事务回滚，共回滚 {} 个事务", rollbackCount);
}
```

**优势**：
- 实现简单，不需要引入 2PC 复杂逻辑
- 性能无额外开销（仅 Leader 切换时有一次扫描）
- 所有操作（增删改）统一为写入，回滚简单

**局限性**：
- 存储膨胀 - 删除操作也写入数据，需要 GC 定期清理
- 最终一致 - Leader 切换后才回滚，短暂时间内可能不一致

### 短期改进（修改现有代码）

1. **增强异常处理**：
   - 异常时记录事务状态到日志
   - 异常时保留内存缓存，允许重试

2. **增强恢复机制**：
   - 重启时检查数据与事务状态的一致性
   - 对于状态 OPEN 但数据已写入的事务，标记为已提交

3. **添加监控告警**：
   - 记录提交失败的事务数量
   - 检测长时间处于 OPEN 状态的事务

### 长期改进（架构优化）

1. 引入两阶段提交协议
2. 实现写前日志（WAL）
3. 添加事务超时和自动回滚机制

## 测试建议

添加以下测试用例验证错误处理：

1. **提交时网络分区测试** - 模拟步骤 3 失败
2. **提交时节点宕机测试** - 模拟步骤 6 前宕机
3. **恢复机制测试** - 重启后验证事务状态正确性
4. **并发提交冲突测试** - 验证冲突检测正确处理
