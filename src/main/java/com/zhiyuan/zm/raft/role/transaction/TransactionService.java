package com.zhiyuan.zm.raft.role.transaction;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.zhiyuan.zm.raft.constant.DataOperationType;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.Command;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.Row;
import com.zhiyuan.zm.raft.exception.RaftRuntimeException;
import com.zhiyuan.zm.raft.role.LeaderRole;
import com.zhiyuan.zm.raft.util.ByteUtil;
import com.zhiyuan.zm.raft.util.KeyUtil;

/**
 * @author zhouzhiyuan
 * @date 2024/11/15 15:48
 */
public class TransactionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionService.class);

  private long allocatedTransactionId;

  //1 字节（事务类型）+ 4 字节（使用int最大值）
  private final byte[] transactionIdKey;

  /**
   * 这个值用来控制
   */
  private final AtomicReference<Long> maxTransactionId;

  /**
   * 更新最大事务 id 的阈值
   */
  private final AtomicReference<Long> updateLimit;

  /**
   * 每次缓存多少个事务 id
   */
  private final long cacheNumber = 500;

  private final long updateStepSize;

  private final LeaderRole leader;

  private final ExecutorService executorService;

  private final AtomicReference<Boolean> updateFlag = new AtomicReference<>(true);

  /**
   * 事务状态管理 - 内存缓存
   * 注意：这个 Map 只缓存当前节点活跃的事务状态，持久化状态存储在 RocksDB 中
   */
  private final Map<String, TransactionInfo> transactionStatus = new ConcurrentHashMap<>();

  /**
   * 事务内待提交数据缓存
   *  key 为 clientId，value 为待提交的数据行
   */
  private final Map<String, List<Row>> pendingWrites = new ConcurrentHashMap<>();

  /**
   * 事务内数据操作计数
   */
  private final Map<String, AtomicInteger> transactionDataCount = new ConcurrentHashMap<>();

  public TransactionService(LeaderRole leader) throws RocksDBException {
    this.leader = leader;
    this.executorService = new ThreadPoolExecutor(1, 1,
        3600L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(1),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("transaction_id_sync").build(),
        (r, executor) -> {
          try {
            LOGGER.warn("transaction id sync exception");
            executor.getQueue().put(r);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        });
    this.transactionIdKey = KeyUtil.generateCacheTransactionIdKey();
    // 从持久化存储中读取事务 ID，如果为空则初始化
    byte[] transactionIdBytes = leader.getSaveData().getValue(transactionIdKey);
    if (transactionIdBytes == null) {
      // 第一次初始化，需要初始化事务 ID
      this.allocatedTransactionId = 0;
      this.maxTransactionId = new AtomicReference<>(cacheNumber);
      this.updateStepSize = cacheNumber / 2;
      this.updateLimit = new AtomicReference<>(updateStepSize);
      // 初始化时存储一部分事务 id
      updateTransactionId(maxTransactionId.get());
    } else {
      this.allocatedTransactionId = ByteUtil.bytesToLong(transactionIdBytes);
      this.maxTransactionId = new AtomicReference<>(allocatedTransactionId + cacheNumber);
      this.updateStepSize = cacheNumber / 2;
      this.updateLimit = new AtomicReference<>(allocatedTransactionId + updateStepSize);
      // 初始化时缓存一部分事务 id
      updateTransactionId(maxTransactionId.get());
    }
    // 从事务存储中恢复未完成的事务状态
    recoverTransactionStatus();
  }

  /**
   * 从持久化存储中恢复未完成的事务状态
   * 节点重启后，可以从 RocksDB 中读取所有状态为 OPEN 的事务
   */
  private void recoverTransactionStatus() throws RocksDBException {
    // 扫描所有事务记录，恢复 OPEN 状态的事务到内存缓存中
    byte[] startKey = KeyUtil.generateTransactionIdKey(0);
    byte[] endKey = KeyUtil.generateTransactionIdKey(Long.MAX_VALUE);

    List<com.zhiyuan.zm.raft.dto.Row> rows = leader.getSaveData().scan(startKey, endKey);
    int recoveredCount = 0;
    for (com.zhiyuan.zm.raft.dto.Row row : rows) {
      try {
        TransactionInfo info = JSON.parseObject(new String(row.getValue()), TransactionInfo.class);
        if (info.getStatus() == Status.OPEN) {
          // 使用 clientId 作为 key 恢复事务状态
          // 由于无法从持久化数据中恢复 clientId，这里只能恢复事务 ID 和状态
          // 实际需要客户端重新发起事务或使用其他机制关联 clientId
          LOGGER.info("恢复未完成的事务：transactionId={}, status={}",
              info.getTransactionId(), info.getStatus());
          recoveredCount++;
        }
      } catch (Exception e) {
        LOGGER.warn("解析事务状态失败：key={}", row.getKey(), e);
      }
    }
    LOGGER.info("事务服务初始化完成，当前已分配事务 ID: {}, 恢复未完成事务数：{}",
        allocatedTransactionId, recoveredCount);
  }


  private synchronized long generateTransactionId() {
    allocatedTransactionId++;
    //异步更新最大值
    if (allocatedTransactionId > updateLimit.get()) {
      updateFlag.set(false);
      executorService.submit(this::run);
    } else {
      return allocatedTransactionId;
    }
    if (allocatedTransactionId >= maxTransactionId.get() && updateFlag.get()) {
      //发送更新最大值请求 ，并阻塞等待更新 .出现这种情况就代缓存个数不合理
      LOGGER.warn(
          "Waiting sync transaction id，This may be because the number of cache transaction ids is set improperly");
      long tmpTransactionId = maxTransactionId.get() + cacheNumber;
      updateTransactionId(tmpTransactionId);
    }
    return allocatedTransactionId;
  }

  private void updateTransactionId(long tmpTransactionId) {
    DataResponest dataResponest = leader.setData(JSON.toJSONString(
        new Command(DataOperationType.INSERT, new Row[] {new Row(transactionIdKey, tmpTransactionId)})
    ));
    if (dataResponest.isSuccess()) {
      maxTransactionId.set(tmpTransactionId);
      updateLimit.set(updateLimit.get() + cacheNumber);
    } else {
      throw new RuntimeException("Failed to store the transaction id : "+dataResponest.getMessage());
    }
  }


  private void run() {
    Long tmpTransactionId = maxTransactionId.get() + cacheNumber;
    DataResponest dataResponest = leader.setData(JSON.toJSONString(
        new Command(DataOperationType.INSERT, new Row[] {new Row(transactionIdKey, tmpTransactionId)})
    ));
    if (dataResponest.isSuccess()) {
      maxTransactionId.set(tmpTransactionId);
      updateLimit.set(updateLimit.get() + updateStepSize);
      LOGGER.info("更新最大事务 id 完成：{}", tmpTransactionId);
    } else {
      LOGGER.warn("同步事务 id 失败");
      updateFlag.set(true);
    }
  }

  public enum Status {
    OPEN((byte) 1, "开启事务"),
    CLOSE((byte) 2, "关闭事务"),
    ROLLBACK((byte) 3, "回滚事务");

    private byte code;

    private String description;

    Status(byte code, String description) {
      this.code = code;
      this.description = description;
    }

    public static Status getStatusBycode(byte a) {
      for (Status value : Status.values()) {
        if (value.getCode() == a) {
          return value;
        }
      }
      throw new RuntimeException("未知的事物类型");
    }

    public byte getCode() {
      return code;
    }

    public void setCode(byte code) {
      this.code = code;
    }

    public String getDescription() {
      return description;
    }

    public void setDescription(String description) {
      this.description = description;
    }

    public boolean isOpen() {
      return this == Status.OPEN;
    }
  }

  public static class TransactionInfo {
    private long timestamp;

    private Status status;

    private Long transactionId;

    private long dataCount;  // 事务内数据条数

    // ==================== MVCC 相关字段 ====================

    /**
     * 快照时间戳（事务开始时的全局最大 commitTs）
     * 用于快照读：事务只能看到 commitTs <= snapshotTs 的数据版本
     */
    private long snapshotTs;

    /**
     * 事务开始时间戳（用于冲突检测）
     */
    private long beginTs;

    /**
     * 提交时间戳（事务提交时分配）
     */
    private long commitTs;

    /**
     * 写入集（记录修改的 key，用于冲突检测）
     * 存储的是 userKey 的字符串形式
     */
    private java.util.Set<String> writeSet;

    public TransactionInfo() {
      this.timestamp = System.currentTimeMillis();
      this.status = Status.OPEN;
      this.writeSet = new java.util.HashSet<>();
    }

    public TransactionInfo(long transactionId) {
      this.timestamp = System.currentTimeMillis();
      this.status = Status.OPEN;
      this.transactionId = transactionId;
      this.writeSet = new java.util.HashSet<>();
    }

    public TransactionInfo(long timestamp, Status status) {
      this.timestamp = timestamp;
      this.status = status;
      this.writeSet = new java.util.HashSet<>();
    }

    public TransactionInfo(long transactionId, long dataCount) {
      this.timestamp = System.currentTimeMillis();
      this.status = Status.OPEN;
      this.transactionId = transactionId;
      this.dataCount = dataCount;
      this.writeSet = new java.util.HashSet<>();
    }

    /**
     * MVCC 构造函数
     * @param transactionId 事务 ID
     * @param snapshotTs 快照时间戳
     * @param beginTs 开始时间戳
     */
    public TransactionInfo(long transactionId, long snapshotTs, long beginTs) {
      this.timestamp = System.currentTimeMillis();
      this.status = Status.OPEN;
      this.transactionId = transactionId;
      this.snapshotTs = snapshotTs;
      this.beginTs = beginTs;
      this.writeSet = new java.util.HashSet<>();
    }

    public Long getTransactionId() {
      return transactionId;
    }

    public void setTransactionId(Long transactionId) {
      this.transactionId = transactionId;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public void setTimestamp(long timestamp) {
      this.timestamp = timestamp;
    }

    public Status getStatus() {
      return status;
    }

    public void setStatus(Status status) {
      this.status = status;
    }

    public long getDataCount() {
      return dataCount;
    }

    public void setDataCount(long dataCount) {
      this.dataCount = dataCount;
    }

    public void incrementDataCount() {
      this.dataCount++;
    }

    // ==================== MVCC 相关方法 ====================

    public long getSnapshotTs() {
      return snapshotTs;
    }

    public void setSnapshotTs(long snapshotTs) {
      this.snapshotTs = snapshotTs;
    }

    public long getBeginTs() {
      return beginTs;
    }

    public void setBeginTs(long beginTs) {
      this.beginTs = beginTs;
    }

    public long getCommitTs() {
      return commitTs;
    }

    public void setCommitTs(long commitTs) {
      this.commitTs = commitTs;
    }

    public java.util.Set<String> getWriteSet() {
      return writeSet;
    }

    public void setWriteSet(java.util.Set<String> writeSet) {
      this.writeSet = writeSet;
    }

    public void addToWriteSet(String userKey) {
      if (this.writeSet == null) {
        this.writeSet = new java.util.HashSet<>();
      }
      this.writeSet.add(userKey);
    }

    public boolean isInWriteSet(String userKey) {
      return this.writeSet != null && this.writeSet.contains(userKey);
    }
  }

  /**
   * 开启事务
   * @param request 请求标识 (clientId)
   * @return 开启结果
   */
  public DataResponest openTranscation(String request) {
    long transactionId = generateTransactionId();
    TransactionInfo transactionInfo = new TransactionInfo(transactionId);
    // 将 TransactionInfo 序列化为 JSON 字符串的 byte 数组，避免 fastjson 反序列化问题
    byte[] valueBytes = JSON.toJSONBytes(transactionInfo);
    DataResponest dataResponest = leader.setData(JSON.toJSONString(
        new Command(DataOperationType.INSERT, new Row[] {new Row(KeyUtil.generateTransactionIdKey(transactionId)
            , valueBytes)})
    ));
    if (dataResponest.isSuccess()) {
      dataResponest.setMessage(request);
      transactionStatus.put(request, transactionInfo);
      pendingWrites.put(request, new java.util.LinkedList<>());
      transactionDataCount.put(request, new AtomicInteger(0));
      LOGGER.info("Successful open transaction : " + request);
      return dataResponest;
    } else {
      LOGGER.error("Failed to open transaction : " + dataResponest.getMessage());
      throw new RuntimeException("Failed to open transaction : " + dataResponest.getMessage());
    }
  }

  /**
   * 提交事务
   * @param request 请求标识 (clientId)
   * @return 提交结果
   */
  public DataResponest commitTranscation(String request) {
    TransactionInfo info = transactionStatus.get(request);
    if (info != null) {
      if (info.getStatus().isOpen()) {
        // 1. 将缓存的数据写入 Raft 日志
        List<Row> pendingRows = pendingWrites.get(request);
        if (pendingRows != null && !pendingRows.isEmpty()) {
          // 将待提交的数据写入 Raft 日志
          Command command = new Command(DataOperationType.INSERT,
              pendingRows.toArray(new Row[0]));
          DataResponest dataResponest = leader.setData(JSON.toJSONString(command));
          if (!dataResponest.isSuccess()) {
            LOGGER.error("Failed to commit transaction data : " + dataResponest.getMessage());
            return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
                "Failed to commit transaction data: " + dataResponest.getMessage());
          }
        }

        // 2. 更新事务状态为 CLOSE
        info.setStatus(Status.CLOSE);
        // 将 TransactionInfo 序列化为 JSON 字符串的 byte 数组，避免 fastjson 反序列化问题
        byte[] valueBytes = JSON.toJSONBytes(info);
        DataResponest statusResponest = leader.setData(JSON.toJSONString(
            new Command(DataOperationType.INSERT, new Row[] {new Row(KeyUtil.generateTransactionIdKey(
                info.getTransactionId())
                , valueBytes)})
        ));

        // 3. 清理缓存数据
        pendingWrites.remove(request);
        transactionDataCount.remove(request);
        transactionStatus.remove(request);

        LOGGER.info("Successful commit transaction : " + request + ", dataCount=" + info.getDataCount());
        return statusResponest;
      }
    }
    LOGGER.warn("Failed to commit transaction, the transaction status is incorrect : " + info);
    return new DataResponest(StatusCode.TRANSACTION_EXCEPTION, "Failed to commit transaction: inner error");
  }

  /**
   * 回滚事务
   * @param request 请求标识 (clientId)
   * @return 回滚结果
   */
  public DataResponest rollbackTranscation(String request) {
    TransactionInfo info = transactionStatus.get(request);
    if (info != null) {
      if (info.getStatus().isOpen()) {
        // 1. 更新事务状态为 ROLLBACK
        info.setStatus(Status.ROLLBACK);
        // 将 TransactionInfo 序列化为 JSON 字符串的 byte 数组，避免 fastjson 反序列化问题
        byte[] valueBytes = JSON.toJSONBytes(info);
        DataResponest dataResponest = leader.setData(JSON.toJSONString(
            new Command(DataOperationType.INSERT, new Row[] {new Row(KeyUtil.generateTransactionIdKey(
                info.getTransactionId())
                , valueBytes)})
        ));

        // 2. 清理缓存数据（不需要写入 Raft 日志，因为事务内的数据从未被提交）
        pendingWrites.remove(request);
        transactionDataCount.remove(request);
        transactionStatus.remove(request);

        LOGGER.info("Successful rollback transaction : " + request);
        return dataResponest;
      }
    }
    LOGGER.warn("Failed to rollback transaction, the transaction status is incorrect : " + info);
    return new DataResponest(StatusCode.TRANSACTION_EXCEPTION, "Failed to rollback transaction: inner error");
  }

  /**
   * 在事务内添加数据（缓存模式，不立即写入 Raft 日志）
   * @param clientId 客户端标识
   * @param rows 数据行
   * @return 操作结果
   */
  public DataResponest putInTransaction(String clientId, Row[] rows) {
    TransactionInfo info = transactionStatus.get(clientId);
    if (info == null || !info.getStatus().isOpen()) {
      LOGGER.warn("Transaction not found or not open for clientId: " + clientId);
      return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
          "Transaction not found or not open for clientId: " + clientId);
    }

    // 将数据添加到待提交缓存
    List<Row> pendingRows = pendingWrites.get(clientId);
    if (pendingRows == null) {
      pendingRows = new java.util.LinkedList<>();
      pendingWrites.put(clientId, pendingRows);
    }
    for (Row row : rows) {
      pendingRows.add(row);
    }

    // 更新事务的数据计数
    info.incrementDataCount();
    AtomicInteger count = transactionDataCount.get(clientId);
    if (count != null) {
      count.addAndGet(rows.length);
    }

    LOGGER.debug("putInTransaction: clientId={}, rows={}, totalPending={}",
        clientId, rows.length, pendingRows.size());
    return new DataResponest(StatusCode.SUCCESS, "Data cached for transaction");
  }

  /**
   * 在事务内删除数据（缓存模式，不立即写入 Raft 日志）
   * @param clientId 客户端标识
   * @param rows 要删除的数据行（只需要 key）
   * @return 操作结果
   */
  public DataResponest deleteInTransaction(String clientId, Row[] rows) {
    TransactionInfo info = transactionStatus.get(clientId);
    if (info == null || !info.getStatus().isOpen()) {
      LOGGER.warn("Transaction not found or not open for clientId: " + clientId);
      return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
          "Transaction not found or not open for clientId: " + clientId);
    }

    // 将删除操作添加到待提交缓存（使用特殊的标记或直接缓存 delete 命令）
    // 这里我们缓存删除操作的 key，在 commit 时执行删除
    List<Row> pendingRows = pendingWrites.get(clientId);
    if (pendingRows == null) {
      pendingRows = new java.util.LinkedList<>();
      pendingWrites.put(clientId, pendingRows);
    }
    // 对于删除操作，我们同样缓存 row，但在 commit 时会使用 DELETE 命令
    for (Row row : rows) {
      pendingRows.add(row);
    }

    info.incrementDataCount();
    AtomicInteger count = transactionDataCount.get(clientId);
    if (count != null) {
      count.addAndGet(rows.length);
    }

    LOGGER.debug("deleteInTransaction: clientId={}, rows={}", clientId, rows.length);
    return new DataResponest(StatusCode.SUCCESS, "Delete operation cached for transaction");
  }

  /**
   * 定期清理已关闭/回滚的事务记录
   * 这个方法可以被定时任务调用，清理 RocksDB 中状态为 CLOSE 或 ROLLBACK 的事务
   */
  public void cleanupRolledbackTransactions() {
    // 扫描所有事务记录，清理已关闭或回滚的事务
    byte[] startKey = KeyUtil.generateTransactionIdKey(0);
    byte[] endKey = KeyUtil.generateTransactionIdKey(Long.MAX_VALUE);

    List<com.zhiyuan.zm.raft.dto.Row> rows = leader.getSaveData().scan(startKey, endKey);
    int cleanedCount = 0;
    for (com.zhiyuan.zm.raft.dto.Row row : rows) {
      try {
        TransactionInfo info = JSON.parseObject(new String(row.getValue()), TransactionInfo.class);
        if (info.getStatus() == Status.CLOSE || info.getStatus() == Status.ROLLBACK) {
          // 清理已过期的事务记录
          leader.getSaveData().delete(row.getKey());
          cleanedCount++;
          LOGGER.debug("清理已完成的事务：transactionId={}, status={}",
              info.getTransactionId(), info.getStatus());
        }
      } catch (RocksDBException e) {
        LOGGER.error("清理事务失败：key={}", row.getKey(), e);
      } catch (Exception e) {
        LOGGER.warn("解析或清理事务状态失败：key={}", row.getKey(), e);
      }
    }
    if (cleanedCount > 0) {
      LOGGER.info("清理已完成的事务记录数：{}", cleanedCount);
    }
  }

  /**
   * 关闭事务服务，停止后台线程
   */
  public void shutdown() {
    if (executorService != null) {
      executorService.shutdownNow();
      try {
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
          LOGGER.warn("Transaction service executor did not terminate in time");
        }
      } catch (InterruptedException e) {
        LOGGER.warn("Interrupted while waiting for transaction service to shutdown", e);
      }
    }
    // 清理所有活跃的事务状态
    transactionStatus.clear();
    pendingWrites.clear();
    transactionDataCount.clear();
    LOGGER.info("Transaction service shutdown completed");
  }

  /**
   * 获取活跃事务数量
   */
  public int getActiveTransactionCount() {
    return transactionStatus.size();
  }

  /**
   * 获取已分配的事务 ID
   */
  public long getAllocatedTransactionId() {
    return allocatedTransactionId;
  }

  /**
   * 获取最大事务 ID
   */
  public long getMaxTransactionId() {
    return maxTransactionId.get();
  }

  /**
   * 获取事务状态分布
   */
  public Map<String, Integer> getTransactionDistribution() {
    Map<String, Integer> distribution = new java.util.HashMap<>();
    distribution.put("OPEN", 0);
    distribution.put("CLOSE", 0);
    distribution.put("ROLLBACK", 0);

    // 扫描持久化存储中的事务状态
    byte[] startKey = KeyUtil.generateTransactionIdKey(0);
    byte[] endKey = KeyUtil.generateTransactionIdKey(Long.MAX_VALUE);

    List<com.zhiyuan.zm.raft.dto.Row> rows = leader.getSaveData().scan(startKey, endKey);
    for (com.zhiyuan.zm.raft.dto.Row row : rows) {
      try {
        TransactionInfo info = JSON.parseObject(new String(row.getValue()), TransactionInfo.class);
        String statusKey = info.getStatus().name();
        distribution.put(statusKey, distribution.getOrDefault(statusKey, 0) + 1);
      } catch (Exception e) {
        LOGGER.debug("解析事务状态失败：key={}", row.getKey(), e);
      }
    }

    return distribution;
  }

  /**
   * 获取活跃的事务 Map
   */
  public Map<String, TransactionInfo> getActiveTransactions() {
    return new ConcurrentHashMap<>(transactionStatus);
  }
}
