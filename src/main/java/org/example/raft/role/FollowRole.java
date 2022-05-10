package org.example.raft.role;

import org.example.conf.GlobalConfig;
import org.example.raft.constant.StatusCode;
import org.example.raft.dto.AddLog;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.GetData;
import org.example.raft.dto.LogEntry;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.VoteRequest;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.service.RaftStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *@author zhouzhiyuan
 *@date 2021/10/27
 */
public class FollowRole extends BaseRole implements Role {

  private static final Logger LOG = LoggerFactory.getLogger(FollowRole.class);

  private long checkTimeoutInterval;

  public FollowRole(SaveData saveData, SaveLog saveLogInterface, RaftStatus raftStatus,RoleStatus roleStatus, GlobalConfig conf) {
    super(saveData,saveLogInterface,raftStatus,roleStatus);
    this.checkTimeoutInterval = conf.getCheckTimeoutInterval();
  }

  /**
   * 如果在超过选举超时时间的情况之前没有收到当前领导人（即该领导人的任期需与这个跟随者的当前任期相同）的心跳/附加日志 状态改为候选人
   */
  @Override
  public void work() {
    raftStatus.setLastTime();
    do {
      try {
        Thread.sleep(checkTimeoutInterval);
        LOG.info("check  heartbeat  ");
        long l = System.currentTimeMillis();
        if (raftStatus.getLastTime() + checkTimeoutInterval < l) {
          //超时 将当前角色转换为 candidate
          roleStatus.followerToCandidate();
          LOG.info("heartbeat timeout  follower role transition candidate role");
        }
      } catch (InterruptedException e) {
        LOG.error(e.getMessage(), e);
      }
    } while (roleStatus.getNodeStatus() == RoleStatus.FOLLOWER);
  }

  @Override
  public RaftRpcResponest addLogRequest(AddLog request) {

    return new RaftRpcResponest(raftStatus.getCurrentTerm(), addLogProcess(request));
  }

  /**
   * follow 状态不参与选举
   * @param request
   * @return
   */
  @Override
  public RaftRpcResponest voteRequest(VoteRequest request) {
    return new RaftRpcResponest(raftStatus.getCurrentTerm(), false);
  }

  @Override
  public DataResponest getData(GetData request) {
     return new DataResponest(StatusCode.REDIRECT,raftStatus.getLeaderAddress());
  }

  @Override
  public DataResponest setData(LogEntry[] request) {
    return new DataResponest(StatusCode.REDIRECT,raftStatus.getLeaderAddress());
  }
}
