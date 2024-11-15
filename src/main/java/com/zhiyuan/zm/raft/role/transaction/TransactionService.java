package com.zhiyuan.zm.raft.role.transaction;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicReference;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.role.Role;

/**
 * @author zhouzhiyuan
 * @date 2024/11/15 15:48
 */
public class TransactionService {

  private static final Logger LOGGER = LoggerFactory.getLogger(TransactionService.class);

  private long transactionId;

  /**
   * 这个值用来控制
   */
  private AtomicReference<Long> maxTransactionId;

  /**
   * 更新最大事务id的阈值
   */
  private AtomicReference<Long> updateLimit;

  /**
   * 每次缓存多少个事务id
   */
  private long catchNumber = 500;

  private long updateStepSize;

  private Role role;

  private ScheduledExecutorService executorService;

  private AtomicReference<Boolean> updateFlag = new AtomicReference<>(true);

  public TransactionService(Role role, ScheduledExecutorService executorService,long transactionId) {
    this.role = role;
    this.executorService = executorService;
    this.transactionId = transactionId;
    this.maxTransactionId = new AtomicReference<>(transactionId + catchNumber);
    this.updateStepSize = catchNumber / 2;
    this.updateLimit = new AtomicReference<>(transactionId + updateStepSize);
  }


  public long generateTransactionId() {
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
      LOGGER.warn("等待同步事务id，这可能是缓存事务id个数设置不合理");
      long tmpTransactionId = maxTransactionId.get()+catchNumber;
      String message = null;
      DataResponest dataResponest = role.setData(message);
      if (dataResponest.isSuccess()) {
        maxTransactionId.set(tmpTransactionId);
        updateLimit.set(updateLimit.get()+catchNumber);
      }else {
        throw new RuntimeException("存储事务id失败");
      }
    }
    return transactionId;
  }


  private void run() {
    long tmpTransactionId = maxTransactionId.get()+catchNumber;
    String message = null;
    DataResponest dataResponest = role.setData(message);
    if (dataResponest.isSuccess()) {
      maxTransactionId.set(tmpTransactionId);
      updateLimit.set(updateLimit.get()+updateStepSize);
      LOGGER.debug("更新最大事务id完成: "+tmpTransactionId);
    } else {
      LOGGER.warn("同步事务id失败");
      updateFlag.set(true);
    }
  }



}
