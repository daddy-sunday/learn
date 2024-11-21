package com.zhiyuan.zm.raft.role.transaction;

import java.util.concurrent.ArrayBlockingQueue;
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

  private long transactionId;

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
    this.transactionIdKey = KeyUtil.generateTransactionIdKey();
    this.transactionId = ByteUtil.bytesToLong(leader.getSaveData().getValue(transactionIdKey));
    this.maxTransactionId = new AtomicReference<>(transactionId + catchNumber);
    this.updateStepSize = catchNumber / 2;
    this.updateLimit = new AtomicReference<>(transactionId + updateStepSize);
  }


  public synchronized long generateTransactionId() {
    transactionId++;
    //异步更新最大值
    if (transactionId > updateLimit.get()) {
      updateFlag.set(false);
      executorService.submit(this::run);
    } else {
      return transactionId;
    }
    if (transactionId >= maxTransactionId.get() && updateFlag.get()) {
      //发送更新最大值请求 ，并阻塞等待更新 .出现这种情况就代缓存个数不合理
      LOGGER.warn("Waiting sync transaction id，This may be because the number of catch transaction ids is set improperly");
      long tmpTransactionId = maxTransactionId.get() + catchNumber;
      DataResponest dataResponest = leader.setData(JSON.toJSONString(
          new Command(DataOperationType.INSERT, new Row[] {new Row(transactionIdKey,tmpTransactionId)})
      ));
      if (dataResponest.isSuccess()) {
        maxTransactionId.set(tmpTransactionId);
        updateLimit.set(updateLimit.get() + catchNumber);
      } else {
        throw new RuntimeException("Failed to store the transaction id ");
      }
    }
    return transactionId;
  }


  private void run() {
    Long tmpTransactionId = maxTransactionId.get() + catchNumber;
    DataResponest dataResponest = leader.setData(JSON.toJSONString(
        new Command(DataOperationType.INSERT, new Row[] {new Row(transactionIdKey,tmpTransactionId)})
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

  //在应用数据时添加事务相关类型
  //提交，不提交
}
