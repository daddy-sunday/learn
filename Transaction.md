# MVCC 事务改造文档

## 一、背景与问题分析

### 当前事务实现的特点

当前 Raft 项目中的事务实现（`TransactionService.java`）采用**基于缓存的两阶段提交**模式：

1. **事务开启** (`openTranscation`)：
   - 生成全局唯一的 `transactionId`（自增 ID，缓存 500 个，异步更新到 RocksDB）
   - 将事务状态（`TransactionInfo`）写入 RocksDB
   - 在内存中维护 `transactionStatus`、`pendingWrites`、`transactionDataCount`

2. **事务内操作** (`putInTransaction` / `deleteInTransaction`)：
   - 数据仅缓存在内存 (`pendingWrites`)，不立即写入 Raft 日志
   - 更新事务计数

3. **事务提交** (`commitTranscation`)：
   - 将 `pendingWrites` 中的所有数据一次性写入 Raft 日志
   - 更新事务状态为 `CLOSE` 并持久化

4. **事务回滚** (`rollbackTranscation`)：
   - 仅更新事务状态为 `ROLLBACK` 并持久化
   - 清理内存缓存（不写入 Raft 日志）

### 当前实现的局限性

| 问题 | 描述 |
|------|------|
| **无并发控制** | 同一时间只有一个事务能提交，不支持多事务并发执行 |
| **无版本管理** | 数据没有版本号，无法实现快照读 |
| **读操作无事务支持** | 当前读操作直接查询 RocksDB，无法读取事务内未提交数据或历史版本 |
| **写冲突检测缺失** | 两个事务同时修改同一 key 时无法检测冲突 |
| **长事务阻塞** | 长事务持有缓存数据期间，其他事务无法看到或提交 |

## 二、MVCC 改造方案

### 核心设计思想

MVCC（多版本并发控制）的核心是：
1. **数据多版本**：每条记录有多个历史版本，每个版本带有事务 ID 或时间戳
2. **快照读**：读操作读取数据的历史版本，不加锁
3. **写操作**：创建新版本数据，不影响正在进行的读操作

### 关键设计点

#### 1. 数据结构改造

**当前数据格式** (`Row` 类)：
```java
public class Row {
    private byte[] key;
    private byte[] value;
}
```

**MVCC 数据格式**（新增）：
```java
public class MVCCVersion {
    private byte[] value;        // 实际数据值
    private long transactionId;  // 创建该版本的事务 ID
    private long commitTs;       // 提交时间戳（用于可见性判断）
    private byte status;         // 版本状态：UNCOMMITTED(0), COMMITTED(1), ABORTED(2)
    private boolean deleted;     // 删除标记
}
```

#### 2. RocksDB 存储结构

**扁平化存储（推荐）**：
- Key：`DATA_KEY_PREFIX + groupId + userKey + transactionId`
- Value：`MVCCVersion`（单个版本）
- 优点：利用 RocksDB 的有序性，便于范围查询和版本清理

#### 3. 事务隔离级别支持

MVCC 天然支持 **快照隔离（Snapshot Isolation）**：

| 操作 | 实现方式 |
|------|----------|
| **Begin** | 分配 `transactionId`，记录 `snapshotTs`（当前最大 committed transactionId） |
| **Read** | 读取 `transactionId <= snapshotTs` 的最大版本 |
| **Write** | 创建新版本，`transactionId = currentTransactionId` |
| **Commit** | 检查写集冲突，无冲突则提交，更新 `lastCommittedTs` |
| **Rollback** | 丢弃所有未提交版本 |

#### 4. 可见性规则

事务 T 只能看到满足以下条件的数据版本 V：
- `V.transactionId < T.transactionId`（在 T 之前提交）
- `V` 的状态为 `COMMITTED`

## 三、改造完成总结

### 一、新增文件

1. **`MVCCVersion.java`** - MVCC 数据版本类
   - 包含 value、transactionId、commitTs、status 字段
   - 支持 UNCOMMITTED、COMMITTED、ABORTED 三种状态
   - 支持删除标记

2. **`MVCCRow.java`** - MVCC 数据行类
   - 扁平化存储模式
   - 包含 userKey 和 MVCCVersion

3. **`MVCCTransactionService.java`** - MVCC 事务服务核心类
   - 实现快照隔离（Snapshot Isolation）
   - 支持多事务并发执行
   - 实现写冲突检测
   - 提供事务开启、提交、回滚、读写操作

4. **`MVCCTransactionTest.java`** - MVCC 单元测试类

### 二、修改文件

1. **`KeyUtil.java`** - 新增 MVCC Key 生成方法
   - `generateMVCCDataKey()` - 生成 MVCC 数据 Key
   - `generateMVCCDataKeyPrefix()` - 生成前缀 Key
   - `generateMVCCDataKeyStart/End()` - 生成范围查询 Key
   - `generateMVCCLastCommitTsKey()` - 生成全局时间戳 Key

2. **`DataOperationType.java`** - 新增 MVCC 操作类型
   - MVCC_WRITE、MVCC_DELETE、MVCC_COMMIT、MVCC_ROLLBACK、MVCC_GC

3. **`TransactionService.TransactionInfo`** - 扩展 MVCC 字段
   - snapshotTs - 快照时间戳
   - beginTs - 开始时间戳
   - commitTs - 提交时间戳
   - writeSet - 写入集（用于冲突检测）

4. **`SaveData.java`** - 新增 MVCC 方法接口
   - `putMVCC()`、`getMVCC()`、`scanMVCCVersions()`
   - `getMVCCByVersion()` - 快照读核心方法
   - `getLastCommittedTs()`、`updateLastCommittedTs()`

5. **`DefaultSaveDataImpl.java`** - 实现 MVCC 方法
   - 实现扁平化存储的读写逻辑
   - 实现快照读的范围查询

6. **`LeaderRole.java`** - 集成 MVCC 事务服务
   - 添加 `mvccTransactionService` 字段
   - 修改 `opentransaction()`、`commitTransaction()`、`rollbackTransaction()` 使用 MVCC 版本
   - 新增 `getInTransaction()` - MVCC 快照读
   - 修改 `putInTransaction()`、`deleteInTransaction()` - MVCC 版本

7. **`BaseRole.java`** - 新增 `getRaftStatus()` 方法

8. **`DataResponse.java`** - 新增 `data` 字段

9. **`StatusCode.java`** - 新增 `NOT_FOUND` 状态码

### 三、核心功能

| 功能 | 说明 |
|------|------|
| **快照读** | 事务读取 snapshotTs 之前已提交的最大版本 |
| **写冲突检测** | 提交时检查是否有其他事务在 T.beginTs 之后提交了相同的 key |
| **读已写** | 事务内能读取自己未提交的写入 |
| **多版本并发** | 支持多事务并发执行，读写不阻塞 |

### 四、使用说明

```java
// 1. 开启事务
DataResponest openResult = mvccTransactionService.openTransaction("clientId");

// 2. 写入数据
DataResponest putResult = mvccTransactionService.putInTransaction(
    "clientId", "key".getBytes(), "value".getBytes());

// 3. 读取数据（快照读）
DataResponest getResult = mvccTransactionService.getInTransaction(
    "clientId", "key".getBytes());

// 4. 删除数据
DataResponest deleteResult = mvccTransactionService.deleteInTransaction(
    "clientId", "key".getBytes());

// 5. 提交事务
DataResponest commitResult = mvccTransactionService.commitTransaction("clientId");

// 6. 回滚事务
DataResponest rollbackResult = mvccTransactionService.rollbackTransaction("clientId");
```

### 五、后续工作（可选）

以下为原计划中可选的后续工作，部分已完成：

1. **垃圾回收机制** - ✅ 已完成
   - ✅ 实现 `MVCCGarbageCollector` 类，定期清理旧版本数据
   - ✅ 定时清理 `commitTs < minActiveTransactionId` 的已提交版本
   - ✅ 利用 RocksDB 的批量写入特性提高性能
   - ✅ 可配置的 GC 间隔时间（默认 1 分钟）
   - ✅ 集成到 `LeaderRole` 中，随 Leader 启动自动运行
   - 待优化：利用 RocksDB 的 Compaction 机制进一步清理

2. **性能优化** - 待完成
   - 使用 Bloom Filter 加速写集查询
   - 优化版本查找的二分查找算法
   - 优化冲突检测算法，使用更高效的数据结构

3. **集成测试** - ✅ 已完成
   - ✅ 创建 `MVCCIntegrationTest.java` 包含以下测试用例：
     - ✅ 单事务正确性：ACID 特性验证（testSingleTransactionBasicFlow）
     - ✅ 事务回滚：验证回滚后数据不被提交（testTransactionRollback）
     - ✅ 快照读验证：并发读写时读取一致性（testSnapshotRead）
     - ✅ 写冲突检测：并发写同一 key 时检测冲突（testConcurrentWriteConflict）
     - ✅ 多事务并发：多事务并发提交测试（testConcurrentTransactions）
     - ✅ 事务内删除操作（testDeleteInTransaction）
     - ✅ 大量数据写入测试（testBulkWriteInTransaction）
   - 待完成：故障恢复测试（节点重启后事务状态正确性）

4. **性能测试** - 待完成
   - 吞吐量对比：改造前后 TPS 对比
   - 延迟测试：不同并发度下的读写延迟
   - 存储膨胀率：MVCC 版本带来的存储开销

## 六、存储格式详解

### MVCC Key 格式

```
Key 格式：1 字节类型 + 4 字节 groupId + N 字节 userKey + 8 字节 transactionId

示例：
- 类型前缀：50 (MVCC_DATA_KEY_PREFIX)
- groupId: 4 字节 int
- userKey: 变长字节数组
- transactionId: 8 字节 long

ByteBuffer 布局：
[1 byte type][4 bytes groupId][N bytes userKey][8 bytes transactionId]
```

### MVCC Value 格式 (JSON 序列化)

```json
{
  "value": [byte array],
  "transactionId": 1001,
  "commitTs": 1001,
  "status": 1,
  "deleted": false
}
```

### 范围查询示例

```java
// 查询 userKey="user:1" 的所有版本
byte[] startKey = KeyUtil.generateMVCCDataKeyStart(groupId, "user:1".getBytes());
byte[] endKey = KeyUtil.generateMVCCDataKeyEnd(groupId, "user:1".getBytes());

// RocksDB 范围查询返回按 transactionId 排序的所有版本
List<MVCCVersion> versions = saveData.scanMVCCVersions(startKey, endKey);

// 获取 snapshotTs=100 时的可见版本
MVCCVersion visibleVersion = saveData.getMVCCByVersion(groupId, "user:1".getBytes(), 100);
```

## 七、事务状态流转

```
OPEN (开启)
  ├──> CLOSE (提交成功)
  └──> ROLLBACK (回滚)
```

### 事务信息结构

```java
public class TransactionInfo {
    private long transactionId;      // 事务 ID
    private long snapshotTs;         // 快照时间戳（读视图）
    private long beginTs;            // 开始时间戳（冲突检测）
    private long commitTs;           // 提交时间戳
    private Status status;           // 事务状态
    private Set<String> writeSet;    // 写入集（key 集合）
    private long dataCount;          // 数据操作计数
}
```

## 八、冲突检测算法

```java
private DataResponest checkConflict(TransactionInfo info) {
    for (Map.Entry<String, TransactionInfo> entry : transactionStatus.entrySet()) {
        TransactionInfo other = entry.getValue();

        // 跳过自己
        if (other.getTransactionId().equals(info.getTransactionId())) {
            continue;
        }

        // 只检查已提交的事务
        if (other.getCommitTs() == 0) {
            continue;
        }

        // 检查是否有写集交集
        if (other.getCommitTs() > info.getBeginTs()) {
            Set<String> intersection = new HashSet<>(info.getWriteSet());
            intersection.retainAll(other.getWriteSet());

            if (!intersection.isEmpty()) {
                return conflict("Write conflict on keys: " + intersection);
            }
        }
    }
    return success();
}
```

## 九、2026-03-05 完成的工作

### 新增文件

1. **`MVCCGarbageCollector.java`** - MVCC 版本垃圾回收器
   - 定期清理已提交的旧版本数据
   - 默认每 1 分钟执行一次 GC
   - 保留 `commitTs >= minActiveTransactionId` 的版本
   - 提供 GC 统计信息（清理版本数、执行时间等）
   - 集成到 `LeaderRole`，随 Leader 启动自动运行

2. **`MVCCIntegrationTest.java`** - MVCC 集成测试类
   - 7 个完整的集成测试用例
   - 覆盖单事务流程、回滚、快照读、冲突检测、并发提交、删除操作、大量数据写入

### 修改文件

1. **`LeaderRole.java`**
   - 添加 `mvccGarbageCollector` 字段
   - 在 `init()` 方法中初始化并启动垃圾回收器
   - 在 `exit()` 方法中关闭垃圾回收器

2. **`Role.java`**
   - 添加 `getInTransaction(String request)` 方法接口

3. **`RoleService.java`**
   - 添加 `GET_IN_TRANSACTION` 消息类型处理

4. **`FollowRole.java`**
   - 实现事务相关方法（重定向到 Leader）
   - 添加 `RedirectAction` 函数式接口

5. **`CandidateRole.java`**
   - 实现事务相关方法（返回选举状态错误）

6. **`LearnerRole.java`**
   - 实现事务相关方法（返回不支持错误）

7. **`ZMClient.java`**
   - 添加 MVCC 事务相关客户端方法：
     - `openTransaction()`
     - `commitTransaction()`
     - `rollbackTransaction()`
     - `putInTransaction()`
     - `getInTransaction()`
     - `deleteInTransaction()`

8. **`InternalRpcClient.java`**
   - 添加内部事务 RPC 方法

9. **`GetData.java`**
   - 添加 `clientId` 字段，支持事务内读取

10. **`MessageType.java`**
    - 添加 `GET_IN_TRANSACTION = 14` 常量

11. **`DataOperationType.java`**
    - 添加 `MVCC_PUT = 12` 常量

12. **`KeyUtil.java`**
    - 将 `MVCC_DATA_KEY_PREFIX` 改为 public

13. **`DefaultSaveDataImpl.java`**
    - 添加 `getRocksDB()` 方法，供 GC 访问底层存储

14. **`Transaction.md`**
    - 更新"后续工作"章节，标记已完成项

---

**改造完成日期**: 2026-03-05
**改造人员**: claude
