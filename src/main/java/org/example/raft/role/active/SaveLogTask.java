package org.example.raft.role.active;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.example.raft.dto.TaskMaterial;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.RoleStatus;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.ByteUtil;
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

  private RoleStatus roleStatus;

  private RaftStatus raftStatus;

  private SaveLog saveLog;

  /**
   * //todo 不是lead以后，还有必要继续写入队列里的数据吗？
   */
  public SaveLogTask(BlockingQueue<TaskMaterial> logQueue, RoleStatus roleStatus,
      RaftStatus raftStatus,
      SaveLog saveLog) {
    this.logQueue = logQueue;
    this.roleStatus = roleStatus;
    this.raftStatus = raftStatus;
    this.saveLog = saveLog;
    executorService = new ScheduledThreadPoolExecutor(1, e -> new Thread(e, "SyscLogTask"));
    executorService.scheduleAtFixedRate(this::run, 0, 50, TimeUnit.MILLISECONDS);
  }

  /**
   * 每50ms写入一次数据
   *
   */
  public void run() {

    int size = logQueue.size();
    if (size < 1) {
      return;
    }

    WriteBatch writeBatch = new WriteBatch();
    TaskMaterial[] taskMaterials = new TaskMaterial[size];
    try {
      for (int i = 0; i < size; i++) {
        TaskMaterial taskDto = logQueue.remove();
        taskMaterials[i] = taskDto;
        writeBatch.put(ByteUtil.concatLogId(raftStatus.getGroupId(), taskDto.getAddLog().getLogIndex()),
            JSON.toJSONBytes(taskDto.getAddLog()));
      }
      saveLog.writBatch(writeBatch);
    } catch (RocksDBException e) {
      LOG.error("写入data失败", e);
      //todo 重试写入，失败后退出？
      System.exit(-1);
    }

    for (int i = 0; i < size; i++) {
      taskMaterials[i].success();
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
