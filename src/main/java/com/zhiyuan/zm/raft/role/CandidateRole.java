package com.zhiyuan.zm.raft.role;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.constant.ServiceStatus;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.AddLogRequest;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.GetData;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.RaftRpcResponest;
import com.zhiyuan.zm.raft.dto.TaskMaterial;
import com.zhiyuan.zm.raft.dto.VoteRequest;
import com.zhiyuan.zm.raft.persistence.SaveData;
import com.zhiyuan.zm.raft.persistence.SaveLog;
import com.zhiyuan.zm.raft.role.active.SaveLogTask;
import com.zhiyuan.zm.raft.role.active.SendVote;
import com.zhiyuan.zm.raft.service.RaftStatus;
import com.zhiyuan.zm.raft.util.RaftUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author zhouzhiyuan
 * @date 2021/10/27
 */
public class CandidateRole extends BaseRole implements Role {

  private static final Logger LOG = LoggerFactory.getLogger(CandidateRole.class);

  public CandidateRole(SaveData saveData, SaveLog saveLogInterface, RaftStatus raftStatus, RoleStatus roleStatus,
      GlobalConfig conf, BlockingQueue<LogEntries[]> applyLogQueue, BlockingQueue<TaskMaterial> saveLogQueue,
      SaveLogTask saveLogTask) {
    super(saveData, saveLogInterface, raftStatus, roleStatus, applyLogQueue, saveLogQueue, saveLogTask, conf);
  }

  /**
   * 在转变成候选人后就立即开始选举过程 自增当前的任期号（currentTerm） 给自己投票 重置选举超时计时器 发送请求投票的 RPC 给其他所有服务器 如果接收到大多数服务器的选票，那么就变成领导人
   * 如果接收到来自新的领导人的附加日志（AppendEntries）RPC，则转变成跟随者(该逻辑在addlog中实现) 如果选举过程超时，则再次发起一轮选举
   */
  @Override
  public void work() {
    //初始化
    ExecutorService executorService = new ThreadPoolExecutor(raftStatus.getPersonelNum() * 2,
        raftStatus.getPersonelNum() * 2,
        0L, TimeUnit.MILLISECONDS,
        new SynchronousQueue<Runnable>());
    List<SendVote> sendVotes = new LinkedList<>();
    VoteRequest request = getVoteRequest(sendVotes);
    raftStatus.setServiceStatus(ServiceStatus.IN_SERVICE);
    //开始选举
    do {
      try {
        LOG.info("candidate->vote: start");
        //置空选举人
        raftStatus.setVotedFor(null);
        //投票结果，默认每次选举都给自己投一票
        int tickets = 1;
        raftStatus.currentTermAddOne();
        request.setTerm(raftStatus.getCurrentTerm());
        //get方法为阻塞方法，所以等待时间放入线程中
        int voteTimeOut = getVoteTimeOut();
        List<Future<Boolean>> futures = executorService
            .invokeAll(sendVotes, voteTimeOut, TimeUnit.MILLISECONDS);
        Thread.sleep(voteTimeOut);
        for (Future<Boolean> booleanFuture : futures) {
          try {
            if (booleanFuture.get()) {
              tickets++;
            }
          } catch (Exception e) {
            //LOG.error(e.getMessage(), e);
          }
        }
        //投票人数过半
        if (raftStatus.getPersonelNum() - tickets < tickets) {
          LOG.info("candidate->vote: vote success,receive tickets: " + tickets + " currentTerm: " + raftStatus
              .getCurrentTerm());
          roleStatus.candidateToLeader();
        } else {
          LOG.info("candidate->vote: vote failed,receive tickets: " + tickets + " currentTerm: " + raftStatus
              .getCurrentTerm());
        }
      } catch (InterruptedException e) {
        LOG.info(e.getMessage(), e);
      }
    } while (roleStatus.getNodeStatus() == RoleStatus.CANDIDATE);
    LOG.info("candidate->vote: end");
    raftStatus.setServiceStatus(ServiceStatus.IN_SWITCH_ROLE);
  }

  private VoteRequest getVoteRequest(List<SendVote> sendVotes) {
    VoteRequest request = new VoteRequest();

    request.setCandidateId(raftStatus.getLocalAddress());
    LogEntries maxLog = saveLog.getMaxLog(RaftUtil.generateLogKey(raftStatus.getGroupId(), Long.MAX_VALUE));
    request.setLastLogIndex(maxLog.getLogIndex());
    request.setLastLogTerm(maxLog.getTerm());

    for (String address : raftStatus.getAllMembers()) {
      sendVotes.add(new SendVote(request, address));
    }
    return request;
  }


  /**
   * 随机151-300
   *
   * @return
   */
  private int getVoteTimeOut() {
    Random r = new Random();
    return r.nextInt(600) % (600 - 300 + 1) + 300;
  }


  @Override
  public RaftRpcResponest addLogRequest(AddLogRequest request) {
    if (request.getTerm() >= raftStatus.getCurrentTerm()) {
      raftStatus.setCurrentTerm(request.getTerm());
      LOG.info("接收到领导人发送的消息，并且term大于等于自己的term，转变选举状态为 跟随者");
      if (roleStatus.candidateToFollower()) {
        raftStatus.setServiceStatus(ServiceStatus.NON_SERVICE);
        raftStatus.setLeaderAddress(request.getLeaderId());
        //等follow初始化完成
      }
    }else {
      if (LOG.isDebugEnabled()) {
        LOG.debug(" 为啥不切换跟随者"+raftStatus+" "+request);
      }
    }
    inServiceWait();
    return addLogProcess(request);
  }

  @Override
  public RaftRpcResponest voteRequest(VoteRequest request) {
    return new RaftRpcResponest(raftStatus.getCurrentTerm(), voteRequestProcess(request));
  }

  @Override
  public DataResponest getData(GetData request) {
    return new DataResponest(StatusCode.SLEEP, "当前服务处于选举状态，不能提供服务，请等待一会重试");
  }

  @Override
  public DataResponest setData(String request) {
    return new DataResponest(StatusCode.SLEEP, "当前服务处于选举状态，不能提供服务，请等待一会重试");
  }

  @Override
  public DataResponest doDataExchange() {
    throw new UnsupportedOperationException("candidate 角色不支持的操作");
  }
}
