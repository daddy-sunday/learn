package org.example.raft.role;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.example.conf.GlobalConfig;
import org.example.raft.constant.MessageType;
import org.example.raft.constant.StatusCode;
import org.example.raft.dto.AddLog;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.GetData;
import org.example.raft.dto.LogEntry;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.TaskMaterial;
import org.example.raft.dto.VoteRequest;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.active.SendHeartbeat;
import org.example.raft.role.active.SyncLogTask;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.ByteUtil;
import org.example.raft.util.RaftUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 *@author zhouzhiyuan
 *@date 2021/10/27
 */
public class LeaderRole extends BaseRole implements Role {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderRole.class);

  private ExecutorService executorService;

  private long sendHeartbeatInterval;

  private long logIndex;

  private long lastTimeTerm;

  private long lastTimeLogIndex;

  private SyncLogTask syncLogTask;

  private BlockingQueue<TaskMaterial> synLogQueue;

  private final byte[] commitLogKey;

  /**
   * lead 繁忙状态 ,可以通过获取 执行线程的队列和线程使用情况来标定繁忙程度，便于外部管理调度
   */
  public long busynessStatus = 0;

  public LeaderRole(SaveData saveData, SaveLog saveLogInterface, RaftStatus raftStatus, RoleStatus roleStatus,
      GlobalConfig conf) {
    super(saveData, saveLogInterface, raftStatus, roleStatus);
    sendHeartbeatInterval = conf.getSendHeartbeatInterval();
    this.commitLogKey = RaftUtil.generateCommitLogKey(raftStatus.getGroupId());
  }

  void init() {
    executorService = new ThreadPoolExecutor(raftStatus.getPersonelNum() * 2, raftStatus.getPersonelNum() * 3,
        3600L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(100), Executors.defaultThreadFactory(), new RejectedExecutionHandler() {
      @Override
      public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
        try {
          executor.getQueue().put(r);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    });
    AddLog maxLog = saveLog.getMaxLog();
    logIndex = maxLog.getLogIndex();
    lastTimeTerm = maxLog.getTerm();
    lastTimeLogIndex = maxLog.getLogIndex();
    //发送同步数据日志
    synLogQueue = new LinkedBlockingDeque<>(1000);
    syncLogTask = new SyncLogTask(synLogQueue, raftStatus, roleStatus);
  }


  /**
   * 发送心跳
   */
  @Override
  public void work() {
    //初始化
    init();

    List<SendHeartbeat> sendHeartbeats = new LinkedList<>();
    RaftRpcRequest request = new RaftRpcRequest(MessageType.LOG, JSON.toJSONString(new AddLog(
        raftStatus.getCurrentTerm(), raftStatus.getLocalAddress())));
    for (String address : raftStatus.getValidMembers()) {
      sendHeartbeats.add(new SendHeartbeat(roleStatus, request, address, raftStatus.getCurrentTerm()));
    }
    //开始发送心跳
    do {
      try {
        LOG.info("send heartbeat");
        executorService.invokeAll(sendHeartbeats);
        Thread.sleep(sendHeartbeatInterval);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    } while (roleStatus.getNodeStatus() == RoleStatus.LEADER);
    executorService.shutdownNow();
    executorService = null;
    syncLogTask.stop();
    syncLogTask = null;
  }

  /**
   * leader 不接受添加日志请求
   * @param request
   * @return
   */
  @Override
  public RaftRpcResponest addLogRequest(AddLog request) {
    return new RaftRpcResponest(raftStatus.getCurrentTerm(), false);
  }

  /**
   * leader不参与选举
   * @param request
   * @return
   */
  @Override
  public RaftRpcResponest voteRequest(VoteRequest request) {
    return new RaftRpcResponest(raftStatus.getCurrentTerm(), false);
  }

  @Override
  public DataResponest getData(GetData request) {
    return null;
  }

  /**
   * @param request
   * @return
   */
  @Override
  public  DataResponest setData(LogEntry[] request) {
    CountDownLatch countDownLatch = new CountDownLatch(2);
    AtomicInteger ticketNum = new AtomicInteger();
    TaskMaterial taskMaterial;

    //此处加同步块的原因是 ，保证存储到队列的中的日志的logIndex是连续递增的
    synchronized (this) {
      logIndex = logIndex + 1;
      long prevLogTerm = lastTimeLogIndex + 1 == logIndex ? lastTimeTerm : raftStatus.getCurrentTerm();
      AddLog addLog = new AddLog(logIndex, raftStatus.getCurrentTerm(), raftStatus.getLocalAddress(), logIndex - 1,
          prevLogTerm, request, raftStatus.getCommitIndex());

      //异步完成实际数据写入
      taskMaterial = new TaskMaterial(addLog, countDownLatch, ticketNum);
      saveLogQueue.add(taskMaterial);
      synLogQueue.add(taskMaterial);
    }
    try {
      //等待异步任务完成
      countDownLatch.wait();
      if (raftStatus.getPersonelNum() - ticketNum.get() < ticketNum.get()) {
        //一批数据提交一次commit
        if (taskMaterial.isCommitLogIndexFlag()) {
          saveLog.saveLog(commitLogKey, ByteUtil.longToBytes(logIndex));
          raftStatus.setCommitIndex(logIndex);
          //将成功提交的日志添加到应用日志队列中
          applyLogQueue.add(taskMaterial.getAddLogs());
        }
        return new DataResponest();
      }
    } catch (InterruptedException e) {
    } catch (Exception e) {
      //todo 直接退出吗？
      System.exit(100);
      LOG.warn("leader -> set data exception  " + e.getMessage());
    }
    return new DataResponest(StatusCode.SYNLOG, "leader->setdata: synchronization log failed");
  }
}
