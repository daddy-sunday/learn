package org.example.raft.role;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.example.conf.GlobalConfig;
import org.example.raft.constant.DataOperationType;
import org.example.raft.constant.MessageType;
import org.example.raft.constant.ServiceStatus;
import org.example.raft.constant.StatusCode;
import org.example.raft.dto.AddLogRequest;
import org.example.raft.dto.ChaseAfterLog;
import org.example.raft.dto.Command;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.GetData;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.SynchronizeLogResult;
import org.example.raft.dto.TaskMaterial;
import org.example.raft.dto.VoteRequest;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.active.ChaseAfterLogTask;
import org.example.raft.role.active.SaveLogTask;
import org.example.raft.role.active.SendHeartbeat;
import org.example.raft.role.active.SyncLogTask;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.ByteUtil;
import org.example.raft.util.RaftUtil;
import org.rocksdb.RocksDBException;
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

  private SyncLogTask syncLogTask;

  private BlockingQueue<TaskMaterial> synLogQueue;

  private ChaseAfterLogTask chaseAfterLogTask;

  private final byte[] datakeyprefix;

  /**
   * lead 繁忙状态 ,可以通过获取 执行线程的队列和线程使用情况来标定繁忙程度，便于外部管理调度
   */
  public long busynessStatus = 0;


  public LeaderRole(SaveData saveData, SaveLog saveLogInterface, RaftStatus raftStatus, RoleStatus roleStatus,
      GlobalConfig conf, BlockingQueue<LogEntries[]> applyLogQueue, BlockingQueue<TaskMaterial> saveLogQueue,
      SaveLogTask saveLogTask) {
    super(saveData, saveLogInterface, raftStatus, roleStatus, applyLogQueue, saveLogQueue, saveLogTask);
    sendHeartbeatInterval = conf.getSendHeartbeatInterval();
    this.datakeyprefix = RaftUtil.generateDataKey(raftStatus.getGroupId());
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
    LogEntries maxLog = saveLog.getMaxLog(RaftUtil.generateLogKey(raftStatus.getGroupId(),Long.MAX_VALUE));
    logIndex = maxLog.getLogIndex();
    synLogQueue = new LinkedBlockingDeque<>(1000);
    syncLogTask = new SyncLogTask(synLogQueue, raftStatus, roleStatus);
    chaseAfterLogTask = new ChaseAfterLogTask(raftStatus, roleStatus, saveLog);

    executorService.submit(new SentFirstLog(maxLog.getTerm()));
  }

  class SentFirstLog implements Runnable{
    private long prevLogTerm;

    public SentFirstLog(long prevLogTerm) {
      this.prevLogTerm = prevLogTerm;
    }

    @Override
    public void run() {
      logIndex += 1;
      //组装发送日志
      LogEntries logEntrie = new LogEntries(logIndex, raftStatus.getCurrentTerm(),
          JSON.toJSONString(new Command(DataOperationType.EMPTY)));
      LogEntries[] logEntries = new LogEntries[] {logEntrie};
      AddLogRequest addLogRequest = new AddLogRequest(logIndex, raftStatus.getCurrentTerm(), raftStatus.getLocalAddress(),
          logIndex - 1,
          prevLogTerm, logEntries, raftStatus.getCommitIndex());
      List<SendHeartbeat> sendHeartbeats = new LinkedList<>();
      RaftRpcRequest request = new RaftRpcRequest(MessageType.LOG, JSON.toJSONString(addLogRequest));
      for (String address : raftStatus.getValidMembers()) {
        sendHeartbeats.add(new SendHeartbeat(roleStatus, request, address, raftStatus.getCurrentTerm()));
      }

      //开始发送日志
      try {
        saveLog.saveLog(RaftUtil.generateLogKey(raftStatus.getGroupId(), logIndex), JSON.toJSONBytes(logEntrie));
        byte count = 1;
        LOG.info("send heartbeat");
        List<Future<SynchronizeLogResult>> futures = executorService
            .invokeAll(sendHeartbeats, 10000, TimeUnit.MILLISECONDS);
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

        if (raftStatus.getPersonelNum() - count < count) {
          raftStatus.setServiceStatus(ServiceStatus.IN_SERVICE);
        } else {
          chaseAfterLogTask.run();
          //追log日志失败了退出执行
          if (raftStatus.getFailedMembers().size() > 0) {
            LOG.error("leader initlog error：synlog  failed");
            System.exit(100);
          }
        }
        chaseAfterLogTask.start();
      } catch (InterruptedException | ExecutionException | RocksDBException e) {
        LOG.error("initlog error：" + e.getMessage());
        //todo  直接退出了?
        System.exit(100);
      }
    }
  }

  /**
   * 发送心跳
   */
  @Override
  public void work() {

    init();

    List<SendHeartbeat> sendHeartbeats = new LinkedList<>();
    RaftRpcRequest request = new RaftRpcRequest(MessageType.LOG, JSON.toJSONString(new AddLogRequest(
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

    exit();
  }

  private void exit() {
    raftStatus.setServiceStatus(ServiceStatus.IN_SWITCH_ROLE);
    executorService.shutdownNow();
    executorService = null;

    syncLogTask.stop();
    syncLogTask = null;
    //清空没有消费的synLogQueue
    while (!synLogQueue.isEmpty()) {
      TaskMaterial poll = synLogQueue.poll();
      if (poll != null) {
        poll.failed();
      }
    }

    chaseAfterLogTask.stop();
    chaseAfterLogTask = null;
    //清空失败队列
    LinkedBlockingDeque<ChaseAfterLog> failedMembers = raftStatus.getFailedMembers();
    while (!failedMembers.isEmpty()) {
      failedMembers.poll();
    }

    stopWriteLog();
  }


  /**
   * leader 不接受添加日志请求
   * @param request
   * @return
   */
  @Override
  public RaftRpcResponest addLogRequest(AddLogRequest request) {
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
    inService();
    byte[] value;
    try {
      value = saveData.getValue(ByteUtil.concatBytes(datakeyprefix, request.getKey().getBytes()));
    } catch (RocksDBException e) {
      LOG.error("查询数据失败：" + e.getMessage(), e);
      return new DataResponest(StatusCode.SYSTEMEXCEPTION, null);
    }
    return new DataResponest(StatusCode.SUCCESS, new String(value));
  }

  /**
   * @param request
   * @return
   */
  @Override
  public DataResponest setData(String request) {
    inService();
    CountDownLatch countDownLatch = new CountDownLatch(2);
    AtomicInteger ticketNum = new AtomicInteger();
    TaskMaterial taskMaterial;
    //此处加同步块的原因是 ，保证存储到队列的中的日志的logIndex是连续递增的
    synchronized (this) {
      logIndex += 1;
      LogEntries[] logEntries = new LogEntries[]{new LogEntries(logIndex, raftStatus.getCurrentTerm(), request)};

      //异步完成实际数据写入
      taskMaterial = new TaskMaterial(logEntries, countDownLatch, ticketNum);
      saveLogQueue.add(taskMaterial);
      synLogQueue.add(taskMaterial);
    }
    try {
      //等待异步任务完成
      countDownLatch.wait();
      if (raftStatus.getPersonelNum() - ticketNum.get() < ticketNum.get()) {
        //一批数据提交一次commit
        if (taskMaterial.isCommitLogIndexFlag()) {
          raftStatus.setCommitIndex(logIndex);
          //将成功提交的日志添加到应用日志队列中
          applyLogQueue.add(taskMaterial.getResult());
        }
        return new DataResponest();
      }
    } catch (InterruptedException e) {
      //todo 直接退出吗？
      LOG.warn("leader -> 存储log时超时被中断" + e.getMessage());
    } catch (Exception e) {
      //todo 直接退出吗？
      System.exit(100);
      LOG.warn("leader -> set data exception  " + e.getMessage());
    }
    return new DataResponest(StatusCode.SYNLOG, "leader->setdata: synchronization log failed");
  }
}
