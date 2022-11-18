package org.example.raft.role;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
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
import org.example.raft.dto.DataChangeDto;
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
import org.example.raft.util.RaftUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author zhouzhiyuan
 * @date 2021/10/27
 */
public class LeaderRole extends BaseRole implements Role {

  private static final Logger LOG = LoggerFactory.getLogger(LeaderRole.class);

  private ExecutorService executorService;

  private long sendHeartbeatInterval;

  private SyncLogTask syncLogTask;

  private BlockingQueue<TaskMaterial> synLogQueue;

  private ChaseAfterLogTask chaseAfterLogTask;

  private long chaseAfterLogTaskInterval;

  private long synLogTaskInterval;

  private volatile long logIndex;

  /**
   * lead 繁忙状态 ,可以通过获取 执行线程的队列和线程使用情况来标定繁忙程度，便于外部管理调度
   */
  public long busynessStatus = 0;

  private List<SendHeartbeat> emptyHeartbeat;

  /**
   * 租约到期时间
   */
  public volatile long leaseEndTime;


  public LeaderRole(SaveData saveData, SaveLog saveLogInterface, RaftStatus raftStatus, RoleStatus roleStatus,
      GlobalConfig conf, BlockingQueue<LogEntries[]> applyLogQueue, BlockingQueue<TaskMaterial> saveLogQueue,
      SaveLogTask saveLogTask) {
    super(saveData, saveLogInterface, raftStatus, roleStatus, applyLogQueue, saveLogQueue, saveLogTask, conf);
    sendHeartbeatInterval = conf.getSendHeartbeatInterval();
    chaseAfterLogTaskInterval = conf.getChaseAfterLogTaskInterval();
    synLogTaskInterval = conf.getSynLogTaskInterval();
    sendHeartbeatTimeout = conf.getSendHeartbeatTimeout();
  }

  void init() {
    executorService = new ThreadPoolExecutor(raftStatus.getPersonelNum() * 2, raftStatus.getPersonelNum() * 3,
        3600L, TimeUnit.MILLISECONDS,
        new ArrayBlockingQueue<Runnable>(100), new ThreadFactoryBuilder().setDaemon(true).setNameFormat("leader").build(),
        new RejectedExecutionHandler() {
          @Override
          public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
            try {
              executor.getQueue().put(r);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        });
    LogEntries maxLog = saveLog.getMaxLog(RaftUtil.generateLogKey(raftStatus.getGroupId(), Long.MAX_VALUE));
    logIndex = maxLog.getLogIndex();
    synLogQueue = new LinkedBlockingDeque<>(1000);
    syncLogTask = new SyncLogTask(synLogQueue, raftStatus, roleStatus, synLogTaskInterval, sendHeartbeatTimeout);
    chaseAfterLogTask = new ChaseAfterLogTask(raftStatus, roleStatus, saveLog, sendHeartbeatTimeout);
    emptyHeartbeat = getEmptyHeartbeats();
    executorService.submit(new SentFirstLog(maxLog.getTerm()));
  }

  class SentFirstLog implements Runnable {
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
      List<SendHeartbeat> sendHeartbeats = getSendHeartbeats(logEntrie);

      //开始发送日志
      try {
        saveLog.saveLog(RaftUtil.generateLogKey(raftStatus.getGroupId(), logIndex), JSON.toJSONBytes(logEntrie));
        int count = 1;
        LOG.debug("发送初始化日志");
        List<Future<SynchronizeLogResult>> futures = executorService
            .invokeAll(sendHeartbeats, 10000, TimeUnit.MILLISECONDS);
        for (Future<SynchronizeLogResult> future : futures) {
          if (future.get().isSuccess()) {
            count++;
          } else {
            //移除同步失败的节点，并使用单独线程追赶落后的日志。
            String address = future.get().getAddress();
            raftStatus.getValidMembers().remove(address);
            //日志不匹配
            if (future.get().getStatusCode() == StatusCode.NOT_MATCH_LOG_INDEX) {
              raftStatus.getFailedMembers().add(new ChaseAfterLog(address, raftStatus.getGroupId(), logIndex - 1));
            } else {
              raftStatus.getFailedMembers().add(new ChaseAfterLog(address, raftStatus.getGroupId(), logIndex));
            }
          }
        }

        if (raftStatus.getPersonelNum() - count > count) {
          int failed = raftStatus.getFailedMembers().size();
          chaseAfterLogTask.run();
          int success = failed - raftStatus.getFailedMembers().size();
          //追log日志失败了退出执行
          count += success;
          if (raftStatus.getPersonelNum() - count > count) {
            LOG.error("leader 初始化 同步log 失败 ");
            System.exit(100);
          }
          LOG.debug("同步日志失败的任务" + raftStatus.getFailedMembers());
          //清空失败的地址
          raftStatus.getFailedMembers().clear();
        }
        LOG.info("同步日志完成");
        raftStatus.setCommitIndex(logIndex);
        commitIndex();
        raftStatus.setServiceStatus(ServiceStatus.IN_SERVICE);
        chaseAfterLogTask.start(chaseAfterLogTaskInterval);
      } catch (InterruptedException | ExecutionException | RocksDBException e) {
        LOG.error("initlog error：" + e.getMessage());
        //todo  直接退出了?
        System.exit(100);
      }
    }

    private List<SendHeartbeat> getSendHeartbeats(LogEntries logEntrie) {
      LogEntries[] logEntries = new LogEntries[] {logEntrie};
      AddLogRequest addLogRequest = new AddLogRequest(logIndex, raftStatus.getCurrentTerm(),
          raftStatus.getLocalAddress(),
          logIndex - 1,
          prevLogTerm, logEntries, raftStatus.getCommitIndex());
      List<SendHeartbeat> sendHeartbeats = new LinkedList<>();
      RaftRpcRequest request = new RaftRpcRequest(MessageType.LOG, JSON.toJSONString(addLogRequest));
      for (String address : raftStatus.getValidMembers()) {
        sendHeartbeats.add(new SendHeartbeat(roleStatus, request, address, sendHeartbeatTimeout));
      }
      return sendHeartbeats;
    }
  }

  /**
   * 发送心跳
   */
  @Override
  public void work() {

    init();
    //开始发送心跳
    do {
      try {
        LOG.info("send heartbeat");
        long start = System.currentTimeMillis();
        if (sendEmptyHeartbeat()) {
          //更新租约时间(lease read 实现)
          leaseEndTime = start + checkTimeoutInterval - 1000;
        }
        long end = System.currentTimeMillis();
        sleep(start, end);
      } catch (InterruptedException e) {
        LOG.error("心跳睡眠中断", e);
      }
    } while (roleStatus.getNodeStatus() == RoleStatus.LEADER);
    exit();
  }

  private boolean validLease() {
    return leaseEndTime > System.currentTimeMillis();
  }

  private void sleep(long start, long end) throws InterruptedException {
    long l = sendHeartbeatInterval - (end - start);
    if (l > 0) {
      Thread.sleep(sendHeartbeatInterval);
    }
  }

  private List<SendHeartbeat> getEmptyHeartbeats() {
    List<SendHeartbeat> emptyHeartbeat = new LinkedList<>();
    RaftRpcRequest request = new RaftRpcRequest(MessageType.LOG, JSON.toJSONString(new AddLogRequest(
        raftStatus.getCurrentTerm(), raftStatus.getLocalAddress())));
    for (String address : raftStatus.getValidMembers()) {
      emptyHeartbeat.add(new SendHeartbeat(roleStatus, request, address, sendHeartbeatTimeout));
    }
    return emptyHeartbeat;
  }

  private void exit() {
    LOG.info("退出leader 状态");
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
   *
   * @param request
   * @return
   */
  @Override
  public RaftRpcResponest addLogRequest(AddLogRequest request) {
    return new RaftRpcResponest(raftStatus.getCurrentTerm(), false);
  }

  /**
   * leader不参与选举
   *
   * @param request
   * @return
   */
  @Override
  public RaftRpcResponest voteRequest(VoteRequest request) {
    return new RaftRpcResponest(raftStatus.getCurrentTerm(), false);
  }

  @Override
  public DataResponest getData(GetData request) {
    if (!inService()) {
      return new DataResponest(StatusCode.NON_SEVICE,
          "服务正在初始化，请等待一会儿重试，状态：" + raftStatus.getServiceStatus());
    }
    if (!validLease()) {
      LOG.error("租约无效，执行readIndex read");
      if (!sendEmptyHeartbeat()) {
        return new DataResponest(StatusCode.RAFT_UNABLE_SERVER, "leader节点不能被大多数节点承认,读取数据失败");
      }
    }

    try {
      waitApplyIndexComplate(raftStatus.getCommitIndex());
      return getDataCommon(request);
    } catch (Exception e) {
      LOG.error("获取leader commitIndex 失败", e);
    }
    return new DataResponest(StatusCode.SYSTEMEXCEPTION, "系统异常：请查看系统日志");
  }


  /**
   * @param request
   * @return
   */
  @Override
  public DataResponest setData(String request) {
    if (!inService()) {
      return new DataResponest(StatusCode.NON_SEVICE,
          "服务正在初始化，请在等待一会重试，状态：" + raftStatus.getServiceStatus());
    }
    CountDownLatch countDownLatch = new CountDownLatch(2);
    AtomicInteger ticketNum = new AtomicInteger();
    TaskMaterial taskMaterial;
    //此处加同步块的原因是 ，保证存储到队列的中的日志的logIndex是连续递增的
    synchronized (this) {
      logIndex += 1;
      LogEntries[] logEntries = new LogEntries[] {new LogEntries(logIndex, raftStatus.getCurrentTerm(), request)};

      //异步完成实际数据写入
      taskMaterial = new TaskMaterial(logEntries, countDownLatch, ticketNum);
      saveLogQueue.add(taskMaterial);
      synLogQueue.add(taskMaterial);
    }
    try {
      //等待异步任务完成
      countDownLatch.await(10000, TimeUnit.MILLISECONDS);
      if (raftStatus.getPersonelNum() - ticketNum.get() < ticketNum.get()) {
        //一批数据提交一次commit
        if (taskMaterial.isCommitLogIndexFlag()) {
          raftStatus.setCommitIndex(logIndex);
          //将成功提交的日志添加到应用日志队列中
          applyLogQueue.add(taskMaterial.getResult());
          if (logIndex == raftStatus.getCommitIndex()) {
            commitIndex();
          }
        }
        return new DataResponest("成功了，哈哈");
      }
    } catch (InterruptedException e) {
      LOG.warn("leader -> 存储log时超时被中断" + e.getMessage());
    } catch (Exception e) {
      //todo 直接退出吗？
      System.exit(100);
      LOG.warn("leader -> set data exception  " + e.getMessage());
    }
    return new DataResponest(StatusCode.SYNLOG, "leader->setdata: synchronization log failed");
  }

  @Override
  public DataResponest doDataExchange() {
    try {
      //优化点：follower 请求commitIndex时先判断当前leader的租约是否有效如果有效就 不发送 空心跳确认leader地位 了。
      if (!validLease()) {
        LOG.error("租约无效，执行readIndex read");
        if (!sendEmptyHeartbeat()) {
          return new DataResponest(StatusCode.SYSTEMEXCEPTION, "leader节点不能被大多数节点承认,读取数据失败");
        }
      }
      return new DataResponest(StatusCode.SUCCESS, JSON.toJSONString(new DataChangeDto(raftStatus.getCommitIndex())));
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    }
    return new DataResponest(StatusCode.SYSTEMEXCEPTION);
  }


  private boolean sendEmptyHeartbeat() {
    List<Future<SynchronizeLogResult>> futures = null;
    int count = 1;
    try {
      futures = executorService
          .invokeAll(emptyHeartbeat, sendHeartbeatTimeout, TimeUnit.MILLISECONDS);
      for (Future<SynchronizeLogResult> future : futures) {
        if (future.get().isSuccess()) {
          count++;
        }
      }
    } catch (InterruptedException | ExecutionException | CancellationException e) {
      LOG.error("发送心跳失败", e);
    }

    if (raftStatus.getPersonelNum() - count < count) {
      return true;
    }
    return false;
  }


  private void commitIndex() throws InterruptedException {
    List<SendHeartbeat> sendHeartbeats = new LinkedList<>();
    RaftRpcRequest request = new RaftRpcRequest(MessageType.LOG, JSON.toJSONString(new AddLogRequest(
        raftStatus.getCurrentTerm(), raftStatus.getLocalAddress(), raftStatus.getCommitIndex())));
    for (String address : raftStatus.getValidMembers()) {
      sendHeartbeats.add(new SendHeartbeat(roleStatus, request, address, sendHeartbeatTimeout));
    }
    executorService.invokeAll(sendHeartbeats, sendHeartbeatTimeout, TimeUnit.MILLISECONDS);
  }
}
