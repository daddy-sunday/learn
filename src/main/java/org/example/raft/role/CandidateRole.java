package org.example.raft.role;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.example.raft.constant.ServiceStatus;
import org.example.raft.constant.StatusCode;
import org.example.raft.dto.AddLogRequest;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.GetData;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.TaskMaterial;
import org.example.raft.dto.VoteRequest;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.active.SaveLogTask;
import org.example.raft.role.active.SendVote;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.RaftUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *@author zhouzhiyuan
 *@date 2021/10/27
 */
public class CandidateRole extends BaseRole implements Role {

  private static final Logger LOG = LoggerFactory.getLogger(CandidateRole.class);

  public CandidateRole(SaveData saveData, SaveLog saveLogInterface, RaftStatus raftStatus, RoleStatus roleStatus,
      BlockingQueue<LogEntries[]> applyLogQueue, BlockingQueue<TaskMaterial> saveLogQueue,
      SaveLogTask saveLogTask) {
    super(saveData, saveLogInterface, raftStatus, roleStatus, applyLogQueue, saveLogQueue, saveLogTask);
  }

  /**
   * 在转变成候选人后就立即开始选举过程
   * 自增当前的任期号（currentTerm）
   * 给自己投票
   * 重置选举超时计时器
   * 发送请求投票的 RPC 给其他所有服务器
   * 如果接收到大多数服务器的选票，那么就变成领导人
   * 如果接收到来自新的领导人的附加日志（AppendEntries）RPC，则转变成跟随者(该逻辑在addlog中实现)
   * 如果选举过程超时，则再次发起一轮选举
   */
  @Override
  public void work() {
    //初始化
    ExecutorService executorService = new ThreadPoolExecutor(raftStatus.getPersonelNum() * 2,
        raftStatus.getPersonelNum() * 2,
        0L, TimeUnit.MILLISECONDS,
        new SynchronousQueue<Runnable>());
    List<SendVote> sendVotes = new LinkedList<>();
    VoteRequest request = new VoteRequest();

    for (String address : raftStatus.getAllMembers()) {
      sendVotes.add(new SendVote(request, address));
    }
    raftStatus.setServiceStatus(ServiceStatus.IN_SERVICE);
    //开始选举
    do {
      try {
        LOG.info("candidate->vote: start");
        //置空选举人
        raftStatus.setVotedFor(null);
        //投票结果，默认每次选举都给自己投一票
        int tickets = 1;
        //todo  并发控制, 我认为这里是可以容忍的更新丢失
        raftStatus.currentTermAddOne();
        getSendVote(request);
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
      } catch (InterruptedException | RocksDBException e) {
        LOG.info(e.getMessage(), e);
      }
    } while (roleStatus.getNodeStatus() == RoleStatus.CANDIDATE);
    LOG.info("candidate->vote: end");
    raftStatus.setServiceStatus(ServiceStatus.IN_SWITCH_ROLE);
  }


  public void getSendVote(VoteRequest request) throws RocksDBException {
    request.setCandidateId(raftStatus.getLocalAddress());
    request.setTerm(raftStatus.getCurrentTerm());
    LogEntries maxLog = saveLog.getMaxLog(RaftUtil.generateLogKey(raftStatus.getGroupId(),Long.MAX_VALUE));
    request.setLastLogIndex(maxLog.getLogIndex());
    request.setLastLogTerm(maxLog.getTerm());
  }

  /**
   * 随机151-300
   * @return
   */
  private int getVoteTimeOut() {
    Random r = new Random();
    return r.nextInt(600) % (600 - 300 + 1) + 300;
  }

  @Override
  public RaftRpcResponest addLogRequest(AddLogRequest request) {
    inService();
    if (request.getTerm() >= raftStatus.getCurrentTerm()) {
      raftStatus.setCurrentTerm(request.getTerm());
      LOG.info("接收到领导人发送的消息，并且term大于等于自己的term，转变选举状态为 跟随者");
      if (roleStatus.candidateToFollower()) {
        raftStatus.setServiceStatus(ServiceStatus.NON_SERVICE);
        raftStatus.setLeaderAddress(request.getLeaderId());
        //等follow初始化完成
        inService();
      }
    }
    return addLogProcess(request);
  }

  @Override
  public RaftRpcResponest voteRequest(VoteRequest request) {
    return new RaftRpcResponest(raftStatus.getCurrentTerm(), voteRequestProcess(request));
  }

  @Override
  public DataResponest getData(GetData request) {
    return new DataResponest(StatusCode.SLEEP, null);
  }

  @Override
  public DataResponest setData(String request) {
    return new DataResponest(StatusCode.SLEEP, null);
  }
}
