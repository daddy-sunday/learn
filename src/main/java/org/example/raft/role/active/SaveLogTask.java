package org.example.raft.role.active;

import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.example.raft.constant.ServiceStatus;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.TaskMaterial;
import org.example.raft.persistence.SaveLog;
import org.example.raft.service.RaftStatus;
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

public class SaveLogTask {

  private static final Logger LOG = LoggerFactory.getLogger(SaveLogTask.class);

  private BlockingQueue<TaskMaterial> logQueue;

  private ScheduledExecutorService executorService;

  private RaftStatus raftStatus;

  private SaveLog saveLog;

  private volatile byte serviceStatus = 0;

  private volatile long execTaskCount = 0;

  /**
   * todo 不是lead以后，还有必要继续写入队列里的数据吗？
   */
  public SaveLogTask(BlockingQueue<TaskMaterial> logQueue,
      RaftStatus raftStatus,
      SaveLog saveLog) {
    this.logQueue = logQueue;
    this.raftStatus = raftStatus;
    this.saveLog = saveLog;
  }

  public void start() {
    executorService = new ScheduledThreadPoolExecutor(1, e -> new Thread(e, "SyscLogTask"));
    executorService.scheduleAtFixedRate(this::run, 0, 50, TimeUnit.MILLISECONDS);
  }

  /**
   * 每50ms写入一次数据
   *
   */
  public void run() {

    serviceStatus = ServiceStatus.IN_SERVICE;
    execTaskCount += 1;

    int size = logQueue.size();
    if (size < 1) {
      serviceStatus = ServiceStatus.NON_SERVICE;
      return;
    }

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
        if (taskMaterials[i]!= null){
          taskMaterials[i].failed();
        }
      }
      serviceStatus = ServiceStatus.NON_SERVICE;
      return;
    }

    for (int i = 0; i < size; i++) {
      taskMaterials[i].success();
    }
    serviceStatus = ServiceStatus.NON_SERVICE;
  }

  public void stop() {
    executorService.shutdownNow();
  }

  public byte getServiceStatus() {
    return serviceStatus;
  }

  public long getExecTaskCount() {
    return execTaskCount;
  }

  /**
   * 任务是否完成
   * @param execTaskCount
   * @return
   */
  public boolean taskComplete(long execTaskCount) {
    if (serviceStatus == ServiceStatus.NON_SERVICE) {
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
