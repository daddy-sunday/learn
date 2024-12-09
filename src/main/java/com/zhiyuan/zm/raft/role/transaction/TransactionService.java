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
   * 更新最大事务id的阈值
   */
  private final AtomicReference<Long> updateLimit;

  /**
   * 每次缓存多少个事务id
   */
  private final long catchNumber = 500;

  private final long updateStepSize;

  private final LeaderRole leader;

  private final ExecutorService executorService;

  private final AtomicReference<Boolean> updateFlag = new AtomicReference<>(true);

  /**
   * 事务状态管理
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
    this.allocatedTransactionId = ByteUtil.bytesToLong(leader.getSaveData().getValue(transactionIdKey));
    this.maxTransactionId = new AtomicReference<>(allocatedTransactionId + catchNumber);
    this.updateStepSize = catchNumber / 2;
    this.updateLimit = new AtomicReference<>(allocatedTransactionId + updateStepSize);
    //初始化时逻辑缓存一部分事物id
    updateTransactionId(maxTransactionId.get());
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
          "Waiting sync transaction id，This may be because the number of catch transaction ids is set improperly");
      long tmpTransactionId = maxTransactionId.get() + catchNumber;
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
      updateLimit.set(updateLimit.get() + catchNumber);
    } else {
      throw new RuntimeException("Failed to store the transaction id ");
    }
  }


  private void run() {
    Long tmpTransactionId = maxTransactionId.get() + catchNumber;
    DataResponest dataResponest = leader.setData(JSON.toJSONString(
        new Command(DataOperationType.INSERT, new Row[] {new Row(transactionIdKey, tmpTransactionId)})
    ));
    if (dataResponest.isSuccess()) {
      maxTransactionId.set(tmpTransactionId);
      updateLimit.set(updateLimit.get() + updateStepSize);
      LOGGER.debug("更新最大事务id完成: " + tmpTransactionId);
    } else {
      LOGGER.warn("同步事务id失败");
      updateFlag.set(true);
    }
  }

  public enum Status {
    OPEN((byte) 1, "开启事务"),
    CLOSE((byte) 2, "关闭事务"),
    ROLLBACK((byte) 3, "回滚事务");

    private byte code;

    private String dscribe;

    Status(byte code, String dscribe) {
      this.code = code;
      this.dscribe = dscribe;
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

    public String getDscribe() {
      return dscribe;
    }

    public void setDscribe(String dscribe) {
      this.dscribe = dscribe;
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

  public DataResponest openTranscation(String reqeust) {
    long transactionId = generateTransactionId();
    TransactionInfo transactionInfo = new TransactionInfo(transactionId);
    DataResponest dataResponest = leader.setData(JSON.toJSONString(
        new Command(DataOperationType.INSERT, new Row[] {new Row(KeyUtil.generateTransactionIdKey(transactionId)
            , transactionInfo)})
    ));
    if (dataResponest.isSuccess()) {
      String transactionCode = UUID.randomUUID().toString();
      dataResponest.setMessage(transactionCode);
      transactionStatus.put(transactionCode, transactionInfo);
      LOGGER.debug("Successful open transacton : " + transactionCode);
      return dataResponest;
    } else {
      LOGGER.error("Failed to open transaction : " + dataResponest.getMessage());
      throw new RuntimeException("Failed to open transaction : " + dataResponest.getMessage());
    }
  }

  public DataResponest commitTranscation(String reqeust) {
    TransactionInfo info = transactionStatus.get(reqeust);
    if (info != null) {
      if (info.getStatus().isOpen()) {
        info.setStatus(Status.CLOSE);
        DataResponest dataResponest = leader.setData(JSON.toJSONString(
            new Command(DataOperationType.INSERT, new Row[] {new Row(KeyUtil.generateTransactionIdKey(
                info.getTransactionId())
                , info)})
        ));
        LOGGER.debug("Successful commit transacton : " + reqeust);
        return dataResponest;
      }
    }
    LOGGER.error("Failed commit transacton,The transaction status is incorrect : " + info);
    return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,"Failed to commit transaction: inner error");
  }

  public DataResponest rollbackTranscation(String reqeust) {
    TransactionInfo info = transactionStatus.get(reqeust);
    if (info != null) {
      if (info.getStatus().isOpen()) {
        info.setStatus(Status.ROLLBACK);
        DataResponest dataResponest = leader.setData(JSON.toJSONString(
            new Command(DataOperationType.INSERT, new Row[] {new Row(KeyUtil.generateTransactionIdKey(
                info.getTransactionId())
                , info)})
        ));
        LOGGER.debug("Successful rollback transacton : " + reqeust);
        return dataResponest;
      }
    }
    LOGGER.error("Failed to rollback transacton,The transaction status is incorrect : " + info);
    return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,"Failed to rollback transaction: inner error");
  }
}
