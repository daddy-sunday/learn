package com.zhiyuan.zm.raft.role;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.extend.UserWork;
import com.zhiyuan.zm.raft.constant.DataOperationType;
import com.zhiyuan.zm.raft.constant.MessageType;
import com.zhiyuan.zm.raft.constant.ServiceStatus;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.AddLogRequest;
import com.zhiyuan.zm.raft.dto.ChaseAfterLog;
import com.zhiyuan.zm.raft.dto.Command;
import com.zhiyuan.zm.raft.dto.ConfigurationChangeDto;
import com.zhiyuan.zm.raft.dto.DataChangeDto;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.GetData;
import com.zhiyuan.zm.raft.dto.LeaderMoveDto;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.RaftInfoDto;
import com.zhiyuan.zm.raft.dto.RaftRpcRequest;
import com.zhiyuan.zm.raft.dto.RaftRpcResponest;
import com.zhiyuan.zm.raft.dto.SynchronizeLogResult;
import com.zhiyuan.zm.raft.dto.TaskMaterial;
import com.zhiyuan.zm.raft.dto.VoteRequest;
import com.zhiyuan.zm.raft.persistence.SaveData;
import com.zhiyuan.zm.raft.persistence.SaveLog;
import com.zhiyuan.zm.raft.role.active.ChaseAfterLogTask;
import com.zhiyuan.zm.raft.role.active.SaveLogTask;
import com.zhiyuan.zm.raft.role.active.SendHeartbeat;
import com.zhiyuan.zm.raft.role.active.SyncLogTask;
import com.zhiyuan.zm.raft.rpc.InternalRpcClient;
import com.zhiyuan.zm.raft.service.RaftStatus;
import com.zhiyuan.zm.raft.util.RaftUtil;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alipay.remoting.exception.RemotingException;
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

  private volatile boolean keepRuning;

  private Thread userWorkthread;


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
        new ArrayBlockingQueue<Runnable>(100),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("leader").build(),
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
    keepRuning = true;
    if (userWorkthread != null) {
      userWorkthread.start();
    }
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
        //看同步log结果是否成功
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
          LOG.debug("初始化同步日志失败的任务" + raftStatus.getFailedMembers());
          //清空失败的地址
          raftStatus.getFailedMembers().clear();
        }else {
          applyLogQueue.add(new LogEntries[]{logEntrie});
        }
        LOG.info("初始化同步日志完成");
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
        if (keepRuning) {
          sleep(start, end);
        } else {
          break;
        }
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
    if (userWorkthread != null) {
      userWorkthread.stop();
    }
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
    if (!canRead()) {
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
    if (raftStatus.getServiceStatus() != ServiceStatus.IN_SERVICE) {
      return new DataResponest(StatusCode.SLEEP,
          "服务正在初始化，请在等待一会重试，状态：" + raftStatus.getServiceStatus());
    }
    CountDownLatch countDownLatch = new CountDownLatch(2);
    AtomicInteger ticketNum = new AtomicInteger();
    TaskMaterial taskMaterial;
    //此处加同步块的原因是 ，保证存储到队列的中的日志的logIndex是连续递增的
    synchronized (this) {
      if (raftStatus.getServiceStatus() != ServiceStatus.IN_SERVICE) {
        return new DataResponest(StatusCode.SLEEP,
            "服务正在初始化，请在等待一会重试，状态：" + raftStatus.getServiceStatus());
      }
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

  /**
   * 将配置消息 转变为共识状态配置（当前配置+最新配置取并集） 应用共识配置？。将共识状态同步到其他节点（通过raft流程）。
   * 新添加节点状态为learnerRole，并使用snapshaot方式追赶日志。当日志追平之后，转为follower节点（这个动作需要leader触发）。
   * 开始新的配置变更（这应该是减少成员变更，否则在上一步应该就结束了）。当发现自己不在成员配置内时，退出leader位置（并认命新leader上线，比自己选leader可控，也更符合实际需求）
   * <p>
   * 当中途失败了，还是会有可能导致配置不一致的问题。这需要怎么解决呢？可能需要有管理控制节点重做或者取消配置变更，使配置保持一致，数据保持一致。
   *
   * @param configurationChangeDto 1
   * @return =
   */
  @Override
  public DataResponest configurationChange(ConfigurationChangeDto configurationChangeDto) {

    return null;
  }

  /**
   * leader 停止对外提供的写服务，可以正常读取数据。 leader 检查 applied Index  等于 longIndex 等于 继位leader applied leader 退位，并通知继位leader 上位
   *
   * @param leaderMoveDto =
   * @return =
   */
  @Override
  public DataResponest leaderMove(LeaderMoveDto leaderMoveDto) {
    if (!validLease() || raftStatus.getServiceStatus() != ServiceStatus.IN_SERVICE) {
      return new DataResponest(StatusCode.LEADER_MOVE, "leader服务状态不符合角色切换条件 ："
          + roleStatus.getNodeStatus() + " " + raftStatus.getServiceStatus());
    }
    if (!raftStatus.getLocalAddress().equals(leaderMoveDto.getOldLeaderAddress())) {
      return new DataResponest(StatusCode.ERROR_REQUEST, "请求节点已经不是leader节点，不能完成后续操作");
    }
    if (raftStatus.getLocalAddress().equals(leaderMoveDto.getNewLeaderAddress())) {
      return new DataResponest(StatusCode.ERROR_REQUEST, "需要切换的目标节点已经是leader节点了");
    }
    synchronized (this) {
      if (roleStatus.getNodeStatus() != RoleStatus.LEADER
          || raftStatus.getServiceStatus() != ServiceStatus.IN_SERVICE) {
        return new DataResponest(StatusCode.LEADER_MOVE,
            "leader服务状态不符合角色切换条件 :" + roleStatus.getNodeStatus() + " " + raftStatus.getServiceStatus());
      }
      LOG.info("接受到leader漂移请求，服务等级降级为只读模式");
      raftStatus.setServiceStatus(ServiceStatus.READ_ONLY);
    }
    //切换leader 超时时间是两倍心跳间隔时间 todo 网络异常时会出现循环时间不可控问题
    int i = (int) (sendHeartbeatInterval / 50);
    do {
      try {
        DataChangeDto dataChangeDto = InternalRpcClient
            .dataChange(leaderMoveDto.getNewLeaderAddress(), sendHeartbeatTimeout, RoleStatus.FOLLOWER);
        if (logIndex == raftStatus.getAppliedIndex() && logIndex == dataChangeDto.getAppliedIndex()) {
          if (roleStatus.leaderToFollower()) {
            keepRuning = false;
            //这个中断可能会导致当前leader 发送心跳失败，但我认为这并不是问题。因为此时leader已经下岗了
            thread.interrupt();
            //通知 继位leader 上位
            LOG.info("通知新leader上位");
            return InternalRpcClient.followerToLeader(leaderMoveDto.getNewLeaderAddress(),
                sendHeartbeatTimeout, leaderMoveDto);
          } else {
            return new DataResponest(StatusCode.ERROR_REQUEST, "执行角色切换命令时时发生了角色切换");
          }
        } else {
          LOG.info("继位leader状态不满足要求："+logIndex+"-"+raftStatus.getAppliedIndex()+"-"+dataChangeDto.getAppliedIndex());
          Thread.sleep(100);
          if (roleStatus.getNodeStatus() != RoleStatus.LEADER) {
            return new DataResponest(StatusCode.LEADER_MOVE, "leader 节点失去领导地位，不能完成后续操作");
          }
          i--;
        }
      } catch (RemotingException | InterruptedException e) {
        LOG.error("获取继位leader 状态失败");
        return new DataResponest(StatusCode.LEADER_MOVE, "获取继位leader 状态失败");
      }
    } while (i > 0);
    if (validLease()) {
      raftStatus.setServiceStatus(ServiceStatus.IN_SERVICE);
    }
    LOG.error("等待继位leader上位超时");
    return new DataResponest(StatusCode.WAIT_TIME_OUT, "等待继位leader上位超时");
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

  @Override
  public DataResponest getRaftInfo() {
    if (validLease()) {
      return new DataResponest(StatusCode.SUCCESS, JSON.toJSONString(new RaftInfoDto(raftStatus.getLocalAddress())));
    }
    return new DataResponest(StatusCode.RAFT_UNABLE_SERVER, "leader节点租约失效，raft service处于不可用状态");
  }

  public void setUserWork(UserWork userWork) {
    userWorkthread = new Thread(userWork);
    userWorkthread.setDaemon(true);
    userWorkthread.setName("use-work");
  }
}
