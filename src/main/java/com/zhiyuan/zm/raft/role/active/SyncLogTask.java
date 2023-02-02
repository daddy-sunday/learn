package com.zhiyuan.zm.raft.role.active;

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

import com.zhiyuan.zm.raft.constant.MessageType;
import com.zhiyuan.zm.raft.dto.AddLogRequest;
import com.zhiyuan.zm.raft.dto.ChaseAfterLog;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.RaftRpcRequest;
import com.zhiyuan.zm.raft.dto.SynchronizeLogResult;
import com.zhiyuan.zm.raft.dto.TaskMaterial;
import com.zhiyuan.zm.raft.role.RoleStatus;
import com.zhiyuan.zm.raft.service.RaftStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author zhouzhiyuan
 * @date 2022/4/26
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

  private int sendHeartbeatTimeout;

  public SyncLogTask(BlockingQueue<TaskMaterial> queue, RaftStatus raftStatus, RoleStatus roleStatus, long interval,
      int sendHeartbeatTimeout) {
    this.raftStatus = raftStatus;
    this.roleStatus = roleStatus;
    this.sendHeartbeatTimeout = sendHeartbeatTimeout;
    executorService = new ThreadPoolExecutor(raftStatus.getPersonelNum(), raftStatus.getPersonelNum(),
        3600L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(100),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("send-log").build(), (r, executor) -> {
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
    scheduler.scheduleAtFixedRate(this::run, 2000, interval, TimeUnit.MILLISECONDS);
  }


  public void run() {
    try {
      int size = queue.size();
      if (size < 1) {
        return;
      }
      LOG.debug("需要同步的log数 " + size);
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

        //发送log给follower
        List<SendHeartbeat> sendHeartbeats = new LinkedList<>();
        for (String address : raftStatus.getValidMembers()) {
          sendHeartbeats.add(
              new SendHeartbeat(roleStatus, new RaftRpcRequest(MessageType.LOG, JSON.toJSONString(addLog)), address,
                  sendHeartbeatTimeout));
        }
        List<Future<SynchronizeLogResult>> futures = executorService
            .invokeAll(sendHeartbeats, sendHeartbeatTimeout, TimeUnit.MILLISECONDS);

        // todo 优化，改为后台等待结果，不能影响发送
        for (Future<SynchronizeLogResult> future : futures) {
          if (future.get().isSuccess()) {
            count++;
          } else {
            //移除同步失败的节点，并使用单独线程追赶落后的日志。
            String address = future.get().getAddress();
            raftStatus.getValidMembers().remove(address);
            ChaseAfterLog chaseAfterLog = new ChaseAfterLog(address, raftStatus.getGroupId(), logIndex);
            raftStatus.getFailedMembers().add(chaseAfterLog);
            LOG.error("日志同步失败，地址添加到失败恢复队列：" + chaseAfterLog);
          }
        }
      } catch (Exception e) {
        LOG.error("发送同步日志异常：" + e.getMessage(), e);
        for (int i = 0; i < size; i++) {
          if (taskMaterials[i] != null) {
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
      LOG.debug("同步log完成");
    } catch (Exception e) {
      LOG.error("存储log日志线程异常: " + e.getMessage(), e);
    }
  }


  public void writeResult() {

  }

  public void stop() {
    scheduler.shutdownNow();
    executorService.shutdownNow();
  }
}