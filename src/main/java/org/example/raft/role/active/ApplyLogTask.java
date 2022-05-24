package org.example.raft.role.active;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.example.raft.dto.LogEntries;
import org.example.raft.persistence.SaveData;
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

  private BlockingQueue<LogEntries[]> logQueue;

  private ScheduledExecutorService executorService;

  private RaftStatus raftStatus;

  private SaveData saveData;

  private final byte[] appliedLogPrefixKey;

  private final byte[] dataKeyPrefix;

  /**
   * //todo 不是lead以后，还有必要继续写入队列里的数据吗？
   * @param dataQueue
   * @param saveData
   */
  public ApplyLogTask(BlockingQueue<LogEntries[]> dataQueue, RaftStatus raftStatus,
      SaveData saveData) {
    this.appliedLogPrefixKey = RaftUtil.generateApplyLogKey(raftStatus.getGroupId());
    this.dataKeyPrefix =  RaftUtil.generateDataKey(raftStatus.getGroupId());
    this.logQueue = dataQueue;
    this.raftStatus = raftStatus;
    this.saveData = saveData;
  }

  public void start(){
    this.executorService = new ScheduledThreadPoolExecutor(1, e -> {
      Thread thread = new Thread(e, "SyscLogTask");
      return thread;
    });
    this.executorService.scheduleAtFixedRate(this::run, 0, 50, TimeUnit.MILLISECONDS);
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
        LogEntries[] addLogs = logQueue.remove();
        saveData.assembleData(writeBatch, addLogs, dataKeyPrefix);
        //找到最后一批数据的最后一个logindex
        if (i == size - 1) {
          logIndex = addLogs[addLogs.length - 1].getLogIndex();
        }
      }

      if (logIndex == 0) {
        //todo  不应该出现的情况 ?
        LOG.error("没有找到 logIndex ，不应该出现的问题");
        System.exit(100);
      }

      //提交 applied id  随批提交应用日志记录，保证原子性
      writeBatch.put(appliedLogPrefixKey, ByteUtil.longToBytes(logIndex));
      saveData.writBatch(writeBatch);
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
