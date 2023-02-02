package com.zhiyuan.zm.raft.service;

import java.util.concurrent.BlockingQueue;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.extend.UserWork;
import com.zhiyuan.zm.raft.constant.MessageType;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.AddLogRequest;
import com.zhiyuan.zm.raft.dto.DataRequest;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.GetData;
import com.zhiyuan.zm.raft.dto.LeaderMoveDto;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.RaftRpcRequest;
import com.zhiyuan.zm.raft.dto.RaftRpcResponest;
import com.zhiyuan.zm.raft.dto.TaskMaterial;
import com.zhiyuan.zm.raft.dto.VoteRequest;
import com.zhiyuan.zm.raft.persistence.SaveData;
import com.zhiyuan.zm.raft.persistence.SaveLog;
import com.zhiyuan.zm.raft.role.CandidateRole;
import com.zhiyuan.zm.raft.role.FollowRole;
import com.zhiyuan.zm.raft.role.LeaderRole;
import com.zhiyuan.zm.raft.role.Role;
import com.zhiyuan.zm.raft.role.RoleStatus;
import com.zhiyuan.zm.raft.role.active.SaveLogTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 *@author zhouzhiyuan
 *@date 2021/10/27
 */
public class RoleService {

  private static final Logger LOG = LoggerFactory.getLogger(RoleService.class);

  private Role followRole;

  private Role leaderRole;

  private Role candidateRole;

  private volatile Role currentRole;

  private RoleStatus roleStatus;

  public RoleService(SaveData saveData, GlobalConfig config, RaftStatus raftStatus, RoleStatus roleStatus,
      SaveLog saveLog, BlockingQueue<LogEntries[]> applyLogQueue,
      BlockingQueue<TaskMaterial> saveLogQueue, SaveLogTask saveLogTask) {
    followRole = new FollowRole(saveData, saveLog, raftStatus, roleStatus, config, applyLogQueue, saveLogQueue,
        saveLogTask);
    leaderRole = new LeaderRole(saveData, saveLog, raftStatus, roleStatus, config, applyLogQueue, saveLogQueue,
        saveLogTask);
    candidateRole = new CandidateRole(saveData, saveLog, raftStatus, roleStatus, config,applyLogQueue, saveLogQueue,
        saveLogTask);
    this.roleStatus = roleStatus;
  }

  public void startWork() {
    while (true) {
      switch (roleStatus.getNodeStatus()) {
        case RoleStatus.FOLLOWER:
          currentRole = followRole;
          followRole.work();
          break;
        case RoleStatus.CANDIDATE:
          currentRole = candidateRole;
          candidateRole.work();
          break;
        case RoleStatus.LEADER:
          currentRole = leaderRole;
          leaderRole.work();
          break;
        default:
      }
    }
  }

  public Object processRaftRequest(RaftRpcRequest request) {
    if (request.getType() == MessageType.LOG) {
      return currentRole.addLogRequest(JSON.parseObject(request.getMessage(), AddLogRequest.class));
    } else if (request.getType() ==MessageType.VOTE) {
      return currentRole.voteRequest(JSON.parseObject(request.getMessage(), VoteRequest.class));
    }
    LOG.error("处理请求类型不支持 request：" + request);
    return new RaftRpcResponest(-1L, false);
  }

  public Object processDataRequest(DataRequest request) {
    if (request.getType() == MessageType.GET) {
      return currentRole.getData(JSON.parseObject(request.getMessage(), GetData.class));
    } else {
      switch (request.getType()) {
        case MessageType.SET:
          return currentRole.setData(request.getMessage());
        case MessageType.READ_INDEX:
          return currentRole.dataExchange(request.getMessage());
        case MessageType.LEADER_MOVE:
          return currentRole.leaderMove(JSON.parseObject(request.getMessage(), LeaderMoveDto.class));
        case MessageType.RAFT_INFO:
          return currentRole.getRaftInfo();
        default:
      }
    }
    LOG.error("处理请求类型不支持 request：" + request);
    return new DataResponest(StatusCode.UNSUPPORT_REQUEST_TYPE, "处理请求类型不支持");
  }

  public void setUserWork(UserWork userWork){
    if (userWork != null){
      LeaderRole tmpLeader =  (LeaderRole)leaderRole;
      tmpLeader.setUserWork(userWork);
    }
  }

}
