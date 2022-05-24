package org.example.raft.role.active;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.example.raft.constant.MessageType;
import org.example.raft.dto.AddLogRequest;
import org.example.raft.dto.ChaseAfterLog;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.SynchronizeLogResult;
import org.example.raft.dto.TaskMaterial;
import org.example.raft.role.RoleStatus;
import org.example.raft.service.RaftStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 *@author zhouzhiyuan
 *@date 2022/4/26
 */
public class SyncLogTask {

  private static final Logger LOG = LoggerFactory.getLogger(SyncLogTask.class);

  /**
   * //定时批量同步log ，目的是合并数据，减少发送次数
   */
  private ScheduledExecutorService scheduler;

  /**
   * 发送log线程池
   */
  private ExecutorService executorService;

  private BlockingQueue<TaskMaterial> queue;

  private RaftStatus raftStatus;

  private RoleStatus roleStatus;

  public SyncLogTask(BlockingQueue<TaskMaterial> queue, RaftStatus raftStatus, RoleStatus roleStatus) {
    this.raftStatus = raftStatus;
    this.roleStatus = roleStatus;

    executorService = new ThreadPoolExecutor(raftStatus.getPersonelNum(), raftStatus.getPersonelNum(),
        3600L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(100), e -> {
      return new Thread(e, "sendLog");
    }, (r, executor) -> {
      try {
        executor.getQueue().put(r);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    });
    this.scheduler = new ScheduledThreadPoolExecutor(1, e -> {
      return new Thread(e, "schedulerSyncLogTask");
    });
    this.queue = queue;
    scheduler.scheduleAtFixedRate(this::run, 0, 50, TimeUnit.MILLISECONDS);
  }


  public void run() {
    int size = queue.size();
    if (size < 1) {
      return;
    }
    TaskMaterial[] taskMaterials = new TaskMaterial[size];
    LogEntries[] logEntries = new LogEntries[size];
    int count = 0;
    try {
      //todo优化  计算发送消息大小。消息太大需要分批发送
      for (int i = 0; i < size; i++) {
        TaskMaterial material = queue.remove();
        taskMaterials[i] = material;
        logEntries[i] = material.getAddLog()[0];
      }

      long logIndex = logEntries[0].getLogIndex();
      //leadeer初始时已经同步过log，所以 preLogTerm 是当前leader的任期
      AddLogRequest addLog = new AddLogRequest(logIndex, raftStatus.getCurrentTerm(), raftStatus.getLocalAddress(),
          logIndex - 1,
          raftStatus.getCurrentTerm(), logEntries, raftStatus.getCommitIndex());

      //发送同步log
      List<SendHeartbeat> sendHeartbeats = new LinkedList<>();
      for (String address : raftStatus.getValidMembers()) {
        sendHeartbeats.add(
            new SendHeartbeat(roleStatus, new RaftRpcRequest(MessageType.LOG, JSON.toJSONString(addLog)), address,
                raftStatus.getCurrentTerm()));
      }

      List<Future<SynchronizeLogResult>> futures = executorService
          .invokeAll(sendHeartbeats, 10000, TimeUnit.MILLISECONDS);

// todo 优化，改为后台登台结果，不能影响发送
      for (Future<SynchronizeLogResult> future : futures) {
        if (future.get().isSuccess()) {
          count++;
        } else {
          //移除同步失败的节点，并使用单独线程追赶落后的日志。
          String address = future.get().getAddress();
          raftStatus.getValidMembers().remove(address);
          raftStatus.getFailedMembers().add(new ChaseAfterLog(address, logIndex));
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      for (int i = 0; i < size; i++) {
        if (taskMaterials[i]!= null) {
          taskMaterials[i].failed();
        }
      }
      return;
    }

    //一批任务更新一次commitLogIndex
    taskMaterials[size - 1].setCommitLogIndexFlag();
    //记录提交的这批数据
    taskMaterials[size - 1].setResult(logEntries);
    for (int i = 0; i < size; i++) {
      taskMaterials[i].success(count);
    }
  }


  public void writeResult(){

  }

  public void stop() {
    scheduler.shutdownNow();
    executorService.shutdownNow();
  }
}
