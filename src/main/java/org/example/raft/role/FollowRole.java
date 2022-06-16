package org.example.raft.role;

import java.util.concurrent.BlockingQueue;

import org.example.conf.GlobalConfig;
import org.example.raft.constant.ServiceStatus;
import org.example.raft.constant.StatusCode;
import org.example.raft.dto.AddLogRequest;
import org.example.raft.dto.DataChangeDto;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.GetData;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.TaskMaterial;
import org.example.raft.dto.VoteRequest;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.active.SaveLogTask;
import org.example.raft.rpc.DefaultRpcClient;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.RaftUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 *@author zhouzhiyuan
 *@date 2021/10/27
 */
public class FollowRole extends BaseRole implements Role {

  private static final Logger LOG = LoggerFactory.getLogger(FollowRole.class);


  public FollowRole(SaveData saveData, SaveLog saveLogInterface, RaftStatus raftStatus, RoleStatus roleStatus,
      GlobalConfig conf, BlockingQueue<LogEntries[]> applyLogQueue, BlockingQueue<TaskMaterial> saveLogQueue,
      SaveLogTask saveLogTask) {
    super(saveData, saveLogInterface, raftStatus, roleStatus, applyLogQueue, saveLogQueue, saveLogTask, conf);
  }

  /**
   * 如果在超过选举超时时间的情况之前没有收到当前领导人（即该领导人的任期需与这个跟随者的当前任期相同）的心跳/附加日志 状态改为候选人
   */

  private void init() {
    //todo 还需要在进行一次状态初始化吗
    LogEntries maxLog = saveLog.getMaxLog(RaftUtil.generateLogKey(raftStatus.getGroupId(),Long.MAX_VALUE));
    //接收log日志时判断日志是否连续使用
    raftStatus.setLastTimeLogIndex(maxLog.getLogIndex());
    raftStatus.setLastTimeTerm(maxLog.getTerm());
    //判断超时选举用
    raftStatus.setLastTime();

    raftStatus.setServiceStatus(ServiceStatus.IN_SERVICE);
  }

  @Override
  public void work() {
    init();
    do {
      try {
        Thread.sleep(checkTimeoutInterval);
        LOG.info("check  heartbeat  ");
        long l = System.currentTimeMillis();
        if (raftStatus.getLastUpdateTime() + checkTimeoutInterval < l) {
          //超时 将当前角色转换为 candidate
          roleStatus.followerToCandidate();
          LOG.info("heartbeat timeout  follower role transition candidate role");
        }
      } catch (InterruptedException e) {
        LOG.error(e.getMessage(), e);
      }
    } while (roleStatus.getNodeStatus() == RoleStatus.FOLLOWER);
    raftStatus.setServiceStatus(ServiceStatus.IN_SWITCH_ROLE);
  }

  @Override
  public RaftRpcResponest addLogRequest(AddLogRequest request) {
    inService();
    return addLogProcess(request);
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
    inService();
    try {
      DataChangeDto dataChangeDto = DefaultRpcClient
          .dataChange(raftStatus.getLeaderAddress(), sendHeartbeatTimeout, RoleStatus.LEADER);
      while (raftStatus.getLastApplied() < dataChangeDto.getCommitIndex()) {
        LOG.debug("appliedIndex <  leader commitIndex : " + raftStatus.getLastApplied() + "<" + dataChangeDto
            .getCommitIndex() + " 等待");
        Thread.sleep(100);
      }
      return getDataCommon(request);
    } catch (Exception e) {
      LOG.error("获取leader commitIndex 失败", e);
    }

    return new DataResponest(StatusCode.REDIRECT, raftStatus.getLeaderAddress());
  }

  @Override
  public DataResponest setData(String request) {
    inService();
    return new DataResponest(StatusCode.REDIRECT, raftStatus.getLeaderAddress());
  }

  @Override
  public DataResponest doDataExchange() {
    return new DataResponest(StatusCode.SUCCESS,
        JSON.toJSONString(new DataChangeDto(raftStatus.getLastTimeLogIndex(), raftStatus.getLastTimeTerm())));
  }
}
