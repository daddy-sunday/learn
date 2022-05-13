package org.example.raft.role;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicInteger;

import org.example.raft.dto.AddLogRequest;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.TaskMaterial;
import org.example.raft.dto.VoteRequest;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.active.ApplyLogTask;
import org.example.raft.role.active.SaveLogTask;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.RaftUtil;
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

  BlockingQueue<LogEntries[]> applyLogQueue;

  BlockingQueue<TaskMaterial> saveLogQueue;

  ApplyLogTask applyLogTask;

  SaveLogTask saveLogTask;


  public BaseRole(SaveData saveData, SaveLog saveLog, RaftStatus raftStatus, RoleStatus roleStatus) {
    this.raftStatus = raftStatus;
    this.roleStatus = roleStatus;
    this.saveLog = saveLog;
    this.saveData = saveData;
    this.applyLogQueue = new LinkedBlockingDeque<>(1000);
    this.applyLogTask = new ApplyLogTask(applyLogQueue, roleStatus, raftStatus, saveData);
    this.saveLogQueue =  new LinkedBlockingDeque<>(1000);
    this.saveLogTask = new SaveLogTask(saveLogQueue, roleStatus, raftStatus, saveLog);
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
   * 目前的实现 当前方法不会被并发访问，因为发送方只有一个leader
   * 接收者的实现：
   *
   * 返回假 如果领导者的任期 小于 接收者的当前任期（译者注：这里的接收者是指跟随者或者候选者）（5.1 节）
   * 返回假 如果接收者日志中没有包含这样一个条目 即该条目的任期在prevLogIndex上能和prevLogTerm匹配上 （译者注：在接收者日志中 如果能找到一个和prevLogIndex以及prevLogTerm一样的索引和任期的日志条目 则继续执行下面的步骤 否则返回假）（5.3 节）
   * 如果一个已经存在的条目和新条目（译者注：即刚刚接收到的日志条目）发生了冲突（因为索引相同，任期不同），那么就删除这个已经存在的条目以及它之后的所有条目 （5.3 节）
   * 追加日志中尚未存在的任何新条目
   * 如果领导者的已知已经提交的最高的日志条目的索引leaderCommit 大于 接收者的已知已经提交的最高的日志条目的索引commitIndex 则把 接收者的已知已经提交的最高的日志条目的索引commitIndex 重置为 领导者的已知已经提交的最高的日志条目的索引leaderCommit 或者是 上一个新条目的索引 取两者的最小值
   *
   */
  boolean addLogProcess(AddLogRequest request) {
    if (request.getTerm() < raftStatus.getCurrentTerm()) {
      LOG.error("receiving log: term of a leader less than  term of a follow");
      return false;
    }
    LogEntries[] entries = request.getEntries();
    if (entries == null) {
      //接收到心跳日志，更新超时时间
      LOG.info("receiving log: term " + request.getTerm());
      raftStatus.setLastTime();
      return true;
    }
    try {
      //优化后的实现逻辑逻辑（只有异常情况（日志不连续时）才需要查询存储系统） 。
      //接收到的日志 小于 等于已经存在的最大日志，则认为日志不连续了
      if (request.getLogIndex() <= raftStatus.getLastTimeLogIndex()) {
        //判断接收到的日志的上一条日志是否匹配
        LogEntries existLog = saveLog.get(RaftUtil.generateLogKey(raftStatus.getGroupId(), request.getPrevLogIndex()));
        if (existLog.getTerm() != request.getPreLogTerm()) {
          LOG.error("receiving log: previous log do not match");
          return false;
        } else {
          LOG.warn("receiving log: log conflict  " + request.toString());
          //todo 修改savelogtask任务，不在消费log。
          //todo 修改savelogtask任务 固定消费从某个key开始的log。
          //清空失效的log
          saveLog.deleteRange(RaftUtil.generateLogKey(raftStatus.getGroupId(), request.getLogIndex()),
              RaftUtil.generateLogKey(raftStatus.getGroupId(), Long.MAX_VALUE));
          //todo 修改savelogtask任务，开始消费log
          //todo 存储的数据需要重做，因为没办法对已经应用的log数据回滚
        }
      } else {
        //判断接收到的log日志是否连续
        if (request.getPrevLogIndex() != raftStatus.getLastTimeLogIndex()
            || request.getPreLogTerm() != raftStatus.getLastTimeTerm()) {
          LOG.error("receiving log: previous log do not match");
          return false;
        }
      }
      CountDownLatch countDownLatch = new CountDownLatch(1);
      AtomicInteger atomicInteger = new AtomicInteger();
      TaskMaterial taskMaterial = new TaskMaterial(entries, countDownLatch, atomicInteger);
      saveLogQueue.add(taskMaterial);
      countDownLatch.wait();

      if (atomicInteger.get() == 1) {
        if (taskMaterial.isCommitLogIndexFlag()) {
          long leaderCommit = request.getLeaderCommit();
          //TODO 这个判断需要吗？
          if (leaderCommit > raftStatus.getCommitIndex()) {
            raftStatus.setCommitIndex(Math.min(leaderCommit, request.getLogIndex()));
          }
          raftStatus.setLastTimeLogIndex(entries[entries.length - 1].getLogIndex());
          raftStatus.setLastTimeTerm(entries[entries.length - 1].getTerm());
          applyLogQueue.add(taskMaterial.getResult());
        }
        return true;
      } else if (atomicInteger.get() > 1) {
        LOG.error("不应该出现的异常 ： follow存储日志时，count > 1 了");
        System.exit(100);
      }
    } catch (RocksDBException | InterruptedException e) {
      LOG.error("follow存储log失败 " + e.getMessage(), e);
      System.exit(100);
    }
    return false;
  }

  @Override
  public abstract DataResponest setData(String request);
}
