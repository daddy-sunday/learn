package com.zhiyuan.zm.raft.role;

import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.constant.ServiceStatus;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.AddLogRequest;
import com.zhiyuan.zm.raft.dto.ConfigurationChangeDto;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.GetData;
import com.zhiyuan.zm.raft.dto.LeaderMoveDto;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.RaftRpcResponest;
import com.zhiyuan.zm.raft.dto.TaskMaterial;
import com.zhiyuan.zm.raft.dto.VoteRequest;
import com.zhiyuan.zm.raft.persistence.SaveData;
import com.zhiyuan.zm.raft.persistence.SaveLog;
import com.zhiyuan.zm.raft.role.active.SaveLogTask;
import com.zhiyuan.zm.raft.service.RaftStatus;
import com.zhiyuan.zm.raft.util.ByteUtil;
import com.zhiyuan.zm.raft.util.RaftUtil;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhouzhiyuan
 * @date 2021/10/29
 */
public abstract class BaseRole implements Role {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRole.class);

  RaftStatus raftStatus;

  RoleStatus roleStatus;

  SaveLog saveLog;

  SaveData saveData;

  BlockingQueue<LogEntries[]> applyLogQueue;

  BlockingQueue<TaskMaterial> saveLogQueue;

  SaveLogTask saveLogTask;

  int sendHeartbeatTimeout;

  long checkTimeoutInterval;

  final byte[] datakeyprefix;

  private int waitTimeInterval = 100;

  private int waitCount = 100;

  Thread thread;
  ReentrantLock lock = new ReentrantLock(true);


  public BaseRole(SaveData saveData, SaveLog saveLog, RaftStatus raftStatus, RoleStatus roleStatus,
      BlockingQueue<LogEntries[]> applyLogQueue,
      BlockingQueue<TaskMaterial> saveLogQueue, SaveLogTask saveLogTask, GlobalConfig conf) {
    this.raftStatus = raftStatus;
    this.roleStatus = roleStatus;
    this.saveLog = saveLog;
    this.saveData = saveData;
    this.applyLogQueue = applyLogQueue;
    this.saveLogQueue = saveLogQueue;
    this.saveLogTask = saveLogTask;
    this.datakeyprefix = RaftUtil.generateDataKey(raftStatus.getGroupId());
    this.checkTimeoutInterval = conf.getCheckTimeoutInterval();
    this.sendHeartbeatTimeout = conf.getSendHeartbeatTimeout();
    waitTimeInterval = conf.getWaitTimeInterval();
    waitCount = conf.getWaitCount();
    thread = Thread.currentThread();
  }

  public boolean inService() {
    if (raftStatus.getServiceStatus() != ServiceStatus.IN_SERVICE) {
      LOG.debug(
          "当前角色不在服务状态，等上一会儿。状态：" + raftStatus.getServiceStatus() + "角色："
              + roleStatus.getNodeStatus());
      return false;
    }
    return true;
  }

  public boolean canRead() {
    if (raftStatus.getServiceStatus() != ServiceStatus.IN_SERVICE
        && raftStatus.getServiceStatus() != ServiceStatus.READ_ONLY) {
      LOG.debug(
          "当前角色不在服务状态，等上一会儿。状态：" + raftStatus.getServiceStatus() + "角色："
              + roleStatus.getNodeStatus());
      return false;
    }
    return true;
  }

  public boolean inServiceWait() {
    int count = 1;
    while (raftStatus.getServiceStatus() != ServiceStatus.IN_SERVICE) {
      try {
        LOG.debug(
            "当前角色不在服务状态，等上一会儿。状态：" + raftStatus.getServiceStatus() + "角色："
                + roleStatus.getNodeStatus());
        Thread.sleep(waitTimeInterval);
      } catch (InterruptedException ignored) {
      }
      if (count >= waitCount) {
        return false;
      }
      count++;
    }
    return true;
  }


  DataResponest getDataCommon(GetData request) {
    try {
      byte[] value = saveData.getValue(ByteUtil.concatBytes(datakeyprefix, request.getKey().getBytes()));
      return new DataResponest(StatusCode.SUCCESS, value == null ? null : new String(value));
    } catch (RocksDBException e) {
      LOG.error("查询数据失败：" + e.getMessage(), e);
      return new DataResponest(StatusCode.SYSTEMEXCEPTION, "服务内部错误，请查看服务器日志");
    }
  }

  /**
   * todo 这个方法的超时 时间需要支持配置
   *
   * @param commitIndex
   * @throws Exception
   */
  void waitApplyIndexComplate(long commitIndex) throws Exception {
    int count = 1;
    while (raftStatus.getAppliedIndex() < commitIndex) {
      LOG.debug("appliedIndex <  leader commitIndex : " + raftStatus.getAppliedIndex() + "<" + commitIndex + " 等待");
      try {
        Thread.sleep(waitTimeInterval);
      } catch (InterruptedException ignored) {
      }
      if (count >= waitCount) {
        throw new TimeoutException("等待applied log 超时 10s");
      }
      count++;
    }
  }

  /**
   * 接收者实现：
   * <p>
   * 如果term < currentTerm返回 false （5.2 节） 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
   *
   * @param request
   * @return
   */
  synchronized boolean voteRequestProcess(VoteRequest request) {
    if (request.getTerm() < raftStatus.getCurrentTerm()) {
      LOG.debug("不能投票: 请求的term小于当前的term ：" + request.getTerm() + "<" + raftStatus.getCurrentTerm());
      return false;
    }
    boolean idVote = raftStatus.getVotedFor() == null || raftStatus.getVotedFor().equals(request.getCandidateId());
    if (idVote) {
      if (request.getLastLogTerm() >= raftStatus.getLastTimeTerm() && request.getLastLogIndex() >= raftStatus
          .getLastTimeLogIndex()) {
        raftStatus.setVotedFor(request.getCandidateId());
        LOG.debug("投票给: " + request.getCandidateId() + " " + request.getTerm());
        return true;
      } else {
        LOG.debug("不能投票，请求的logIndex小于当前的logindex：" + request.getLastLogIndex() + ">="
            + raftStatus.getCommitIndex());
      }
    }
    LOG.debug("已经投过票了,不能重复投票：" + raftStatus.getVotedFor());
    return false;
  }

  /**
   * 目前的实现 当前方法不会被并发访问，因为发送方只有一个leader 接收者的实现：
   * <p>
   * 返回假 如果领导者的任期 小于 接收者的当前任期（译者注：这里的接收者是指跟随者或者候选者）（5.1 节） 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在prevLogIndex上能和prevLogTerm匹配上
   * （译者注：在接收者日志中 如果能找到一个和prevLogIndex以及prevLogTerm一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假）（5.3 节）
   * 如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节） 追加日志中尚未存在的任何新条目
   * 如果领导者的已知已经提交的最高的日志条目的索引leaderCommit 大于 接收者的已知已经提交的最高的日志条目的索引commitIndex 则把 接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为
   * 领导者的已知已经提交的最高的日志条目的索引leaderCommit 或者是 上一个新条目的索引 取两者的最小值
   * <p>
   * todo 可以优化的地方：leader 发送日志时不判断成功失败一直发送。但是客户端这边需要加公平锁 ，实现上一条日志处理完成才能处理下一条日志。保证处理日志顺序一致
   */
  RaftRpcResponest addLogProcess(AddLogRequest request) {

    if (request.getTerm() < raftStatus.getCurrentTerm()) {
      LOG.error("接收日志：接收到的term小于当前term " + request.getTerm() + "<" + raftStatus.getCurrentTerm());
      return new RaftRpcResponest(raftStatus.getCurrentTerm(), false, StatusCode.MIN_TERM);
    }
    LogEntries[] entries = request.getEntries();
    if (entries == null) {
      LOG.debug("接收心跳日志: 更新follow超时时间");
      raftStatus.setLastTime();
      if (raftStatus.getCurrentTerm() != request.getTerm()) {
        LOG.debug("接收心跳日志: 更新currentTerm=" + request.getTerm() + " leaderAddress=" + request.getLeaderId());
        raftStatus.setCurrentTerm(request.getTerm());
        raftStatus.setLeaderAddress(request.getLeaderId());
      }
      //优化点：log静止时 follower 的commitIndex总是落后于leader。原因是commitIndex 是通过add log 同步的，每次发送log时只能知道上次的log是commit成功的。
      //当心跳中的commitIndex 等于最后收到的日志条目并且term相等时（日志静止时的条件），更新本地commitIndex。
      if (request.getLeaderCommit() == raftStatus.getLastTimeLogIndex()
          && request.getTerm() == raftStatus.getLastTimeTerm()) {
        LOG.debug("接收心跳日志: 更新commitIndex=" + request.getLeaderCommit());
        raftStatus.setCommitIndex(request.getLeaderCommit());
      }
      return new RaftRpcResponest(raftStatus.getCurrentTerm(), true, StatusCode.EMPTY);
    }
    lock.lock();
    try {
      //优化后的实现逻辑逻辑（只有异常情况（日志不连续时）才需要查询存储系统） 。
      //接收到的日志 小于 等于已经存在的最大日志，则认为日志不连续了
      if (request.getLogIndex() <= raftStatus.getLastTimeLogIndex()) {
        //判断接收到的日志的上一条日志是否匹配
        LogEntries existLog = saveLog.get(RaftUtil.generateLogKey(raftStatus.getGroupId(), request.getPrevLogIndex()));
        if (existLog.getTerm() != request.getPreLogTerm()) {
          LOG.error("接收日志的上一条log 的term不匹配。 请求的term" + request.getPreLogTerm() + " 当前的term："
              + existLog.getTerm());
          return new RaftRpcResponest(raftStatus.getCurrentTerm(), false, StatusCode.NOT_MATCH_LOG_INDEX);
        } else {
          LOG.warn("日志冲突 " + request + " " + raftStatus);
          raftStatus.setServiceStatus(ServiceStatus.WAIT_RENEW);
          //停止写log任务，这个操作没有应该也可以。理论上在一次term中日志一定是一直连续的，只有刚开始初始化时才会出现
          waitQueueIsEmpty();
          //清空失效的log
          saveLog.deleteRange(RaftUtil.generateLogKey(raftStatus.getGroupId(), request.getLogIndex()),
              RaftUtil.generateLogKey(raftStatus.getGroupId(), Long.MAX_VALUE));
          raftStatus.setServiceStatus(ServiceStatus.IN_SERVICE);
        }
      } else {
        //判断接收到的log日志是否连续
        if (request.getPrevLogIndex() != raftStatus.getLastTimeLogIndex()
            || request.getPreLogTerm() != raftStatus.getLastTimeTerm()) {
          LOG.error("接收日志不匹配" + request + " 预期的值: " + raftStatus.getLastTimeLogIndex() + " "
              + raftStatus
              .getLastTimeTerm());
          return new RaftRpcResponest(raftStatus.getCurrentTerm(), false, StatusCode.NOT_MATCH_LOG_INDEX);
        }
      }

      CountDownLatch cyclicBarrier = new CountDownLatch(1);
      AtomicInteger atomicInteger = new AtomicInteger();
      TaskMaterial taskMaterial = new TaskMaterial(entries, cyclicBarrier, atomicInteger);
      saveLogQueue.add(taskMaterial);
      //更新最后收到的log index 和term，这可以用来判断下次收到的日志是否连续。
      raftStatus.setLastTimeLogIndex(entries[entries.length - 1].getLogIndex());
      raftStatus.setLastTimeTerm(entries[entries.length - 1].getTerm());
      if (LOG.isDebugEnabled()) {
        LOG.debug("添加log日志到队列中，更新preLogIndex 和 preTerm：" + entries[entries.length - 1].getLogIndex()
            + " , " + entries[entries.length - 1].getTerm());
      }
      cyclicBarrier.await(10000, TimeUnit.MILLISECONDS);
      if (atomicInteger.get() == 1) {
        if (taskMaterial.isCommitLogIndexFlag()) {
          long leaderCommit = request.getLeaderCommit();
          if (leaderCommit > raftStatus.getCommitIndex()) {
            raftStatus.setCommitIndex(Math.min(leaderCommit, request.getLogIndex()));
          }
          LOG.debug("添加日志到应用队列 ：" + request.getLogIndex());
          applyLogQueue.add(taskMaterial.getResult());
        }
        LOG.debug("日志接收成功 ：" + request.getLogIndex());
        return new RaftRpcResponest(raftStatus.getCurrentTerm(), true, StatusCode.EMPTY);
      } else if (atomicInteger.get() > 1) {
        LOG.error("不应该出现的异常 ： follow存储日志时，count > 1 了");
        System.exit(100);
      }
    } catch (RocksDBException e) {
      LOG.error("follow存储log失败 " + e.getMessage(), e);
      System.exit(100);
    } catch (InterruptedException e) {
      LOG.error("follow存储log时超时被中断 " + e.getMessage(), e);
    }finally {
      lock.unlock();
    }
    LOG.debug("接收日志异常 ：" + request.getLogIndex());
    return new RaftRpcResponest(raftStatus.getCurrentTerm(), false, StatusCode.SERVICE_EXCEPTION);
  }

  @Override
  public abstract DataResponest setData(String request);

  @Override
  public DataResponest dataExchange(String request) {
    if (roleStatus.getNodeStatus() != Integer.parseInt(request, 10)) {
      LOG.warn("当前角色：" + roleStatus.getNodeStatus() + " 请求的角色：" + Integer.parseInt(request, 10));
      return new DataResponest(StatusCode.UNSUPPORT_REQUEST_FUNCATION, "当前角色发生切换，不能正常响应消息");
    }
    return doDataExchange();
  }

  public abstract DataResponest doDataExchange();

  /**
   * 当发出的请求没有被大多数节点承认时，或者说只有少部分节点收到了消息，则这些消息在队列中就是垃圾消息需要清理掉
   * 清理的时机就是节点角色切换时
   */
  protected void clearAppliedQueue(){
    while (!applyLogQueue.isEmpty()){
      LogEntries[] peek = applyLogQueue.peek();
      long logIndex = peek[0].getLogIndex();
      if (logIndex > raftStatus.getCommitIndex()) {
        LogEntries[] addLogs = applyLogQueue.remove();
        LOG.info("清理应用队列 ："+ Arrays.toString(addLogs));
      }else {
        break;
      }
    }
  }

  protected void waitQueueIsEmpty() {
    while (!saveLogQueue.isEmpty()) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    //等待一次savelogtask完成,保证当前raft日志是静止的
    long execTaskCount = saveLogTask.getExecTaskCount();
    while (!saveLogTask.taskComplete(execTaskCount)) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public DataResponest snapshaotCopy(String request) {
    return new DataResponest(StatusCode.RAFT_UNABLE_SERVER, "当前节点状态不支持该操作");
  }

  @Override
  public DataResponest configurationChange(ConfigurationChangeDto configurationChangeDto) {
    return new DataResponest(StatusCode.RAFT_UNABLE_SERVER, "当前节点状态不支持该操作");
  }

  @Override
  public DataResponest leaderMove(LeaderMoveDto leaderMoveDto) {
    return new DataResponest(StatusCode.RAFT_UNABLE_SERVER, "当前节点状态不支持该操作");
  }

  @Override
  public DataResponest getRaftInfo() {
    return new DataResponest(StatusCode.RAFT_UNABLE_SERVER, "当前节点状态不支持该操作");
  }
}
