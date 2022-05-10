package org.example.raft.role;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.example.raft.dto.AddLog;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.LogEntry;
import org.example.raft.dto.TaskMaterial;
import org.example.raft.dto.VoteRequest;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.active.ApplyLogTask;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.ByteUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *@author zhouzhiyuan
 *@date 2021/10/29
 */
public abstract class BaseRole implements Role {

  private static final Logger LOG = LoggerFactory.getLogger(BaseRole.class);

  RaftStatus raftStatus;

  RoleStatus roleStatus;

  SaveLog saveLog;

  SaveData saveData;

  BlockingQueue<AddLog[]> applyLogQueue;

  BlockingQueue<TaskMaterial> saveLogQueue;

  ApplyLogTask consumerDataQueue;


  public BaseRole(SaveData saveData, SaveLog saveLog, RaftStatus raftStatus, RoleStatus roleStatus) {
    this.raftStatus = raftStatus;
    this.roleStatus = roleStatus;
    this.saveLog = saveLog;
    this.saveData = saveData;
    applyLogQueue = new LinkedBlockingDeque<>(1000);
    consumerDataQueue = new ApplyLogTask(applyLogQueue, roleStatus, raftStatus, saveData, saveLog);
  }

  /**
   * 接收者实现：
   *
   * 如果term < currentTerm返回 false （5.2 节）
   * 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
   * @param request
   * @return
   */
  synchronized boolean voteRequestProcess(VoteRequest request) {
    if (request.getTerm() < raftStatus.getCurrentTerm()) {
      return false;
    }
    if ((raftStatus.getVotedFor() == null || raftStatus.getVotedFor().equals(request.getCandidateId()))
        && request.getLastLogIndex() >= raftStatus.getCommitIndex()) {
      raftStatus.setVotedFor(request.getCandidateId());
      LOG.info("投票给: " + request.getCandidateId() + " " + request.getTerm());
      return true;
    }
    return false;
  }

  /**
   * 接收者的实现：
   *
   * 返回假 如果领导者的任期 小于 接收者的当前任期（译者注：这里的接收者是指跟随者或者候选者）（5.1 节）
   * 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在prevLogIndex上能和prevLogTerm匹配上 （译者注：在接收者日志中 如果能找到一个和prevLogIndex以及prevLogTerm一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假）（5.3 节）
   * 如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
   * 追加日志中尚未存在的任何新条目
   * 如果领导者的已知已经提交的最高的日志条目的索引leaderCommit 大于 接收者的已知已经提交的最高的日志条目的索引commitIndex 则把 接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为 领导者的已知已经提交的最高的日志条目的索引leaderCommit 或者是 上一个新条目的索引 取两者的最小值
   *
   */
  boolean addLogProcess(AddLog request) {
    if (request.getTerm() < raftStatus.getCurrentTerm()) {
      LOG.error("receiving log: term of a leader less than  term of a follow");
      return false;
    }
    LogEntry[] entries = request.getEntries();
    if (entries == null) {
      //接收到心跳日志，更新超时时间
      LOG.info("receiving log: term " + request.getTerm());
      raftStatus.setLastTime();
      return true;
    }
    try {

      AddLog addLog = saveLog.get(ByteUtil.concatLogId(raftStatus.getGroupId(), request.getPrevLogIndex()));
      if (addLog == null && addLog.getTerm() != request.getPreLogTerm()) {
        LOG.error("receiving log: previous log do not match");
        return false;
      }
      byte[] key = ByteUtil.concatLogId(raftStatus.getGroupId(), request.getLogIndex());
      AddLog existLog = saveLog.get(key);
      if (existLog != null) {
        //日志存在并且term不同，需要删除日志并重做 状态机中的数据。
        if (existLog.getTerm() != request.getTerm()) {
          LOG.warn("receiving log: log conflict  " + addLog.toString());
          saveLog.deleteRange(key, ByteUtil.concatLogId(raftStatus.getGroupId(), Long.MAX_VALUE));
          //todo 发出指令-》数据重做
        } else {
          return true;
        }
      }
      saveLog.saveLog(key, request);

      long leaderCommit = request.getLeaderCommit();
      // todo  是否有并发修改 commitIndex的可能
      if (leaderCommit > raftStatus.getCommitIndex()) {
        raftStatus.setCommitIndex(Math.min(leaderCommit, request.getLogIndex()));
      }
      return true;
    } catch (RocksDBException e) {
      LOG.error(e.getMessage(), e);
    }
    return false;
  }

  @Override
  public abstract DataResponest setData(LogEntry[] request);
}
