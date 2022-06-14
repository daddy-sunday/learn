package org.example.raft.role.active;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.example.conf.GlobalConfig;
import org.example.raft.dto.LogEntries;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveIterator;
import org.example.raft.persistence.SaveLog;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.ByteUtil;
import org.example.raft.util.RaftUtil;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

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

  private SaveLog saveLog;

  private final byte[] appliedLogPrefixKey;

  private final byte[] dataKeyPrefix;

  private long interval;


  /**
   * //todo 不是lead以后，还有必要继续写入队列里的数据吗？
   * @param dataQueue
   * @param saveData
   */
  public ApplyLogTask(BlockingQueue<LogEntries[]> dataQueue, RaftStatus raftStatus,
      SaveData saveData, SaveLog saveLog, GlobalConfig config) {
    this.appliedLogPrefixKey = RaftUtil.generateApplyLogKey(raftStatus.getGroupId());
    this.dataKeyPrefix = RaftUtil.generateDataKey(raftStatus.getGroupId());
    this.logQueue = dataQueue;
    this.raftStatus = raftStatus;
    this.saveData = saveData;
    this.interval = config.getApplyLogTaskInterval();
  }

  public void start() {
    this.executorService = new ScheduledThreadPoolExecutor(1, e -> {
      return new Thread(e, "SyscLogTask");
    });
    this.executorService.scheduleAtFixedRate(this::run, 0, interval, TimeUnit.MILLISECONDS);
  }



  /**
   * 每50ms写入一次数据
   *
   */
  public void run() {

    int size = logQueue.size();
    if (1 > size) {
      // 无数据写入
      LOG.debug("未监测到log需要应用: " + size);
      return;
    }
    LogEntries[] peek = logQueue.peek();
    long longIndex = peek[0].getLogIndex();
    if (longIndex > raftStatus.getCommitIndex()) {
      LOG.debug("跳过执行： longIndex > commitIndex ");
      return;
    }

    try {
      //todo 第一次运行时会有应用日志不连续的问题，有时间可以优化到初始化中
      if (raftStatus.getLastApplied() + 1 != longIndex) {
        if (applyLog()) {
          LOG.error("应用log日志时出现逻辑错误");
          System.exit(100);
        }
      }
      LOG.debug("检查到需要存储的任务数: " + size);
      WriteBatch writeBatch = new WriteBatch();
      long logIndex = 0;
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
    LOG.debug("应用log日志完成");
  }

  /**
   * 恢复没有被应用的log
   * @return
   * @throws RocksDBException
   */
  private boolean applyLog() throws RocksDBException {
    long commitIndex = raftStatus.getCommitIndex();
    long lastApplied = raftStatus.getLastApplied();
    byte[] bytes = RaftUtil.generateDataKey(raftStatus.getGroupId());
    if (lastApplied < commitIndex) {
      LOG.info("恢复应用日志："+lastApplied+" -> "+commitIndex);
      SaveIterator scan = saveLog.scan(RaftUtil.generateLogKey(raftStatus.getGroupId(), lastApplied),
          RaftUtil.generateLogKey(raftStatus.getGroupId(), lastApplied));
      //todo 优化 数据量很大时有内存溢出的风险
      WriteBatch writeBatch = new WriteBatch();
      for (scan.seek(); scan.valied(); scan.next()) {
        byte[] value = scan.getValue();
        LogEntries[] entries = JSON.parseObject(value, LogEntries[].class);
        saveData.assembleData(writeBatch, entries, bytes);
      }
      //提交 applied id  随批提交应用日志记录，保证原子性
      writeBatch.put(RaftUtil.generateApplyLogKey(raftStatus.getGroupId()), ByteUtil.longToBytes(commitIndex));
      saveData.writBatch(writeBatch);
      LOG.info("应用日志完成：" + lastApplied + " -> " + commitIndex);
      return true;
    }else {
      return false;
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
