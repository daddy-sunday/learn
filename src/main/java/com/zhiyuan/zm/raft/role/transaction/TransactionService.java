package com.zhiyuan.zm.raft.role.transaction;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
    // TODO: 实现从事务存储中恢复未完成的事务状态
    // 目前事务状态已经通过 KeyUtil.generateTransactionIdKey(transactionId) 持久化到 RocksDB
    // 节点重启后可以扫描所有事务记录，恢复 OPEN 状态的事务到内存缓存中
    LOGGER.info("事务服务初始化完成，当前已分配事务 ID: {}", allocatedTransactionId);
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
      throw new RuntimeException("Failed to store the transaction id ");
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

    public TransactionInfo() {
      this.timestamp = System.currentTimeMillis();
      this.status = Status.OPEN;
    }

    public TransactionInfo(long transactionId) {
      this.timestamp = System.currentTimeMillis();
      this.status = Status.OPEN;
      this.transactionId = transactionId;
    }

    public TransactionInfo(long timestamp, Status status) {
      this.timestamp = timestamp;
      this.status = status;
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
  }

  /**
   * 开启事务
   * @param request 请求标识
   * @return 开启结果
   */
  public DataResponest openTranscation(String request) {
    long transactionId = generateTransactionId();
    TransactionInfo transactionInfo = new TransactionInfo(transactionId);
    DataResponest dataResponest = leader.setData(JSON.toJSONString(
        new Command(DataOperationType.INSERT, new Row[] {new Row(KeyUtil.generateTransactionIdKey(transactionId)
            , transactionInfo)})
    ));
    if (dataResponest.isSuccess()) {
      dataResponest.setMessage(request);
      transactionStatus.put(request, transactionInfo);
      LOGGER.info("Successful open transaction : " + request);
      return dataResponest;
    } else {
      LOGGER.error("Failed to open transaction : " + dataResponest.getMessage());
      throw new RuntimeException("Failed to open transaction : " + dataResponest.getMessage());
    }
  }

  /**
   * 提交事务
   * @param request 请求标识
   * @return 提交结果
   */
  public DataResponest commitTranscation(String request) {
    TransactionInfo info = transactionStatus.get(request);
    if (info != null) {
      if (info.getStatus().isOpen()) {
        info.setStatus(Status.CLOSE);
        DataResponest dataResponest = leader.setData(JSON.toJSONString(
            new Command(DataOperationType.INSERT, new Row[] {new Row(KeyUtil.generateTransactionIdKey(
                info.getTransactionId())
                , info)})
        ));
        LOGGER.info("Successful commit transaction : " + request);
        return dataResponest;
      }
    }
    LOGGER.warn("Failed to commit transaction, the transaction status is incorrect : " + info);
    return new DataResponest(StatusCode.TRANSACTION_EXCEPTION, "Failed to commit transaction: inner error");
  }

  /**
   * 回滚事务
   * @param request 请求标识
   * @return 回滚结果
   */
  public DataResponest rollbackTranscation(String request) {
    TransactionInfo info = transactionStatus.get(request);
    if (info != null) {
      if (info.getStatus().isOpen()) {
        info.setStatus(Status.ROLLBACK);
        DataResponest dataResponest = leader.setData(JSON.toJSONString(
            new Command(DataOperationType.INSERT, new Row[] {new Row(KeyUtil.generateTransactionIdKey(
                info.getTransactionId())
                , info)})
        ));
        LOGGER.info("Successful rollback transaction : " + request);
        return dataResponest;
      }
    }
    LOGGER.warn("Failed to rollback transaction, the transaction status is incorrect : " + info);
    return new DataResponest(StatusCode.TRANSACTION_EXCEPTION, "Failed to rollback transaction: inner error");
  }
}
