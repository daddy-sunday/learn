package org.example.raft.role.active;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.example.raft.dto.AddLog;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.RoleStatus;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.ByteUtil;
import org.example.raft.util.RaftUtil;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *@author zhouzhiyuan
 *@date 2021/11/30
 * 存储日志  并应用到 数据存储库
 */

public class ApplyLogTask {

  private static final Logger LOG = LoggerFactory.getLogger(ApplyLogTask.class);

  private BlockingQueue<AddLog[]> logQueue;

  private ScheduledExecutorService executorService;

  private RoleStatus roleStatus;

  private RaftStatus raftStatus;

  private SaveData saveData;

  private SaveLog saveLog;

  private final byte[] appliedLogKey;

  /**
   * //todo 不是lead以后，还有必要继续写入队列里的数据吗？
   * @param dataQueue
   * @param roleStatus
   * @param saveData
   */
  public ApplyLogTask(BlockingQueue<AddLog[]> dataQueue, RoleStatus roleStatus, RaftStatus raftStatus,
      SaveData saveData, SaveLog saveLog) {
    this.appliedLogKey = RaftUtil.generateApplyLogKey(raftStatus.getGroupId());
    this.logQueue = dataQueue;
    this.roleStatus = roleStatus;
    this.raftStatus = raftStatus;
    this.saveData = saveData;
    this.saveLog = saveLog;
    executorService = new ScheduledThreadPoolExecutor(1, e -> {
      Thread thread = new Thread(e, "SyscLogTask");
      return thread;
    });
    executorService.scheduleAtFixedRate(this::run, 0, 50, TimeUnit.MILLISECONDS);
  }

  /**
   * 每50ms写入一次数据
   *
   */
  public void run() {

    int size = logQueue.size();
    if (1 > size) {
      // 无数据写入
      return;
    }

    WriteBatch writeBatch = new WriteBatch();
    long logIndex = 0;
    try {
      for (int i = 0; i < size; i++) {
        AddLog[] addLogs = logQueue.remove();
        for (int j = 0; j < addLogs.length; j++) {
          saveData.assembleData(writeBatch, addLogs[i]);
        }
        //找到最后一批数据的最后一个logindex
        if (i == size - 1) {
          logIndex = addLogs[addLogs.length - 1].getLogIndex();
        }
      }
      saveData.writBatch(writeBatch);

      if (logIndex == 0) {
        //todo  重试写入失败后退出程序 ?
        System.exit(100);
      }

      //提交 applied id
      saveLog.saveLog(appliedLogKey, ByteUtil.longToBytes(logIndex));
      raftStatus.setLastApplied(logIndex);
    } catch (RocksDBException e) {
      LOG.error("写入data失败", e);
      //todo  重试写入失败后退出程序 ?
      System.exit(100);
    }
  }

  public void stop() {
    executorService.shutdown();
    try {
      //todo 需要优化等待时间
      executorService.awaitTermination(100, TimeUnit.DAYS);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
