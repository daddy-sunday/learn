package org.example.raft.service;

import java.util.concurrent.BlockingQueue;

import org.example.conf.GlobalConfig;
import org.example.raft.constant.MessageType;
import org.example.raft.dto.AddLogRequest;
import org.example.raft.dto.DataRequest;
import org.example.raft.dto.GetData;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.TaskMaterial;
import org.example.raft.dto.VoteRequest;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.CandidateRole;
import org.example.raft.role.FollowRole;
import org.example.raft.role.LeaderRole;
import org.example.raft.role.Role;
import org.example.raft.role.RoleStatus;
import org.example.raft.role.active.SaveLogTask;
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
    candidateRole = new CandidateRole(saveData, saveLog, raftStatus, roleStatus, applyLogQueue, saveLogQueue,
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
    } else if (request.getType() ==MessageType.SET) {
      return currentRole.setData(request.getMessage());

    }
    LOG.error("处理请求类型不支持 request：" + request);
    return new RaftRpcResponest(-1L, false);
  }

}
