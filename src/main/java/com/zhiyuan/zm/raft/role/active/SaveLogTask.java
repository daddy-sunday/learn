package com.zhiyuan.zm.raft.role.active;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.constant.ServiceStatus;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.TaskMaterial;
import com.zhiyuan.zm.raft.persistence.SaveLog;
import com.zhiyuan.zm.raft.service.RaftStatus;
import com.zhiyuan.zm.raft.util.RaftUtil;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author zhouzhiyuan
 * @date 2021/11/30 存储日志  并应用到 数据存储库
 */

public class SaveLogTask {

  private static final Logger LOG = LoggerFactory.getLogger(SaveLogTask.class);

  private BlockingQueue<TaskMaterial> logQueue;

  private ScheduledExecutorService executorService;

  private RaftStatus raftStatus;

  private SaveLog saveLog;


  /**
   * 这个状态代表当前任务是否正在运行，
   */
  private volatile byte busyStatus = 0;

  private volatile long execTaskCount = 0;

  private long interval;

  /**
   * todo 不是lead以后，还有必要继续写入队列里的数据吗？
   */
  public SaveLogTask(BlockingQueue<TaskMaterial> logQueue,
      RaftStatus raftStatus,
      SaveLog saveLog, GlobalConfig config) {
    this.logQueue = logQueue;
    this.raftStatus = raftStatus;
    this.saveLog = saveLog;
    interval = config.getSavelogTaskInterval();
  }

  public void start() {
    executorService = new ScheduledThreadPoolExecutor(1,
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("SyscLogTask").build());
    executorService.scheduleAtFixedRate(this::run, 2000, interval, TimeUnit.MILLISECONDS);
  }

  /**
   * 每50ms写入一次数据
   */
  public void run() {
    try {
      busyStatus = ServiceStatus.IN_SERVICE;
      execTaskCount += 1;

      int size = logQueue.size();
      if (size < 1) {
        busyStatus = ServiceStatus.NON_SERVICE;
        return;
      }
      LOG.debug("检查到需要存储的任务数: " + size);

      WriteBatch writeBatch = new WriteBatch();
      TaskMaterial[] taskMaterials = new TaskMaterial[size];
      try {
        for (int i = 0; i < size; i++) {
          TaskMaterial taskDto = logQueue.remove();
          taskMaterials[i] = taskDto;
          LogEntries[] addLog = taskDto.getAddLog();
          for (LogEntries logEntries : addLog) {
            writeBatch.put(RaftUtil.generateLogKey(raftStatus.getGroupId(), logEntries.getLogIndex()),
                JSON.toJSONBytes(logEntries));
          }
        }
        saveLog.writBatch(writeBatch);
      } catch (RocksDBException e) {
        LOG.error("写入data失败", e);
        //todo 重试写入，失败后退出？
        System.exit(-1);
      } catch (NoSuchElementException e) {
        LOG.warn("队列中没有数据了，可能是raft发生了角色切换");
        for (int i = 0; i < size; i++) {
          if (taskMaterials[i] != null) {
            taskMaterials[i].failed();
          }
        }
        busyStatus = ServiceStatus.NON_SERVICE;
        return;
      }
      //一批任务更新一次commitLogIndex
      taskMaterials[size - 1].setCommitLogIndexFlag();
      //记录提交的这批数据
      taskMaterials[size - 1].setResult(concatLogEntries(taskMaterials));
      for (int i = 0; i < size; i++) {
        taskMaterials[i].success();
      }
      busyStatus = ServiceStatus.NON_SERVICE;
      LOG.debug("存储log日志完成");
    } catch (Exception e) {
      LOG.error("存储log日志线程异常: " + e.getMessage(), e);
    }
  }

  private LogEntries[] concatLogEntries(TaskMaterial[] taskMaterials) {
    int size = 0;
    for (int i = 0; i < taskMaterials.length; i++) {
      size = size + taskMaterials[i].getAddLog().length;
    }
    LogEntries[] entries = new LogEntries[size];
    int length = 0;
    for (int i = 0; i < taskMaterials.length; i++) {
      System.arraycopy(taskMaterials[i].getAddLog(), 0, entries, length, taskMaterials[i].getAddLog().length);
      length += taskMaterials[i].getAddLog().length;
    }
    return entries;
  }


  public void stop() {
    executorService.shutdownNow();
  }

  public byte getServiceStatus() {
    return busyStatus;
  }

  public long getExecTaskCount() {
    return execTaskCount;
  }

  /**
   * 任务是否完成
   *
   * @param execTaskCount
   * @return
   */
  public boolean taskComplete(long execTaskCount) {
    if (busyStatus == ServiceStatus.NON_SERVICE) {
      return true;
    } else {
      if (this.execTaskCount > execTaskCount) {
        return true;
      } else {
        return false;
      }
    }
  }
}
