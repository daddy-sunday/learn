package org.example.raft.role.active;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.example.raft.constant.MessageType;
import org.example.raft.dto.AddLog;
import org.example.raft.dto.ChaseAfterLog;
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
      Thread thread = new Thread(e, "sendLog");
      return thread;
    }, new RejectedExecutionHandler() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        try {
          executor.getQueue().put(r);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    this.scheduler = new ScheduledThreadPoolExecutor(1, e -> {
      Thread thread = new Thread(e, "schedulerSyncLogTask");
      return thread;
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
    AddLog[] addLogs = new AddLog[size];
    for (int i = 0; i < size; i++) {
      TaskMaterial material = queue.remove();
      taskMaterials[i] = material;
      addLogs[i] = material.getAddLog();
    }
    //发送同步log
    List<SendHeartbeat> sendHeartbeats = new LinkedList<>();
    for (String address : raftStatus.getValidMembers()) {
      sendHeartbeats.add(
          new SendHeartbeat(roleStatus, new RaftRpcRequest(MessageType.LOG, JSON.toJSONString(addLogs)), address,
              raftStatus.getCurrentTerm()));
    }
    int count = 0;
    try {
      List<Future<SynchronizeLogResult>> futures = executorService
          .invokeAll(sendHeartbeats, 10000, TimeUnit.MILLISECONDS);
      for (Future<SynchronizeLogResult> future : futures) {
        if (future.get().isSuccess()) {
          count++;
        } else {
          //移除同步失败的节点，并使用单独线程追赶落后的日志。
          String address = future.get().getAddress();
          raftStatus.getValidMembers().remove(address);
          raftStatus.getFailedMembers().add(new ChaseAfterLog(address, addLogs[0].getLogIndex()));
        }
      }
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }

    //一批任务更新一次commitLogIndex
    taskMaterials[size - 1].setCommitLogIndexFlag();
    //记录提交的这批数据
    taskMaterials[size - 1].setAddLogs(addLogs);
    for (int i = 0; i < size; i++) {
      taskMaterials[i].success(count);
    }
  }


  public void stop() {
    scheduler.shutdownNow();
    executorService.shutdownNow();
  }
}
