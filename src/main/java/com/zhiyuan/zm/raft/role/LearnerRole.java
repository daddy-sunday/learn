package com.zhiyuan.zm.raft.role;

import java.util.concurrent.BlockingQueue;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.extend.UserWork;
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
import com.zhiyuan.zm.raft.service.RaftStatus;

/**
 * @author zhouzhiyuan
 * @date 2022/11/21 16:29
 */
public class LearnerRole extends BaseRole{


  public LearnerRole(SaveData saveData, SaveLog saveLog,
      RaftStatus raftStatus, RoleStatus roleStatus,
      BlockingQueue<LogEntries[]> applyLogQueue,
      BlockingQueue<TaskMaterial> saveLogQueue,
      SaveLogTask saveLogTask, GlobalConfig conf) {
    super(saveData, saveLog, raftStatus, roleStatus, applyLogQueue, saveLogQueue, saveLogTask, conf);
  }

  @Override
  public void work() {

  }

  @Override
  public RaftRpcResponest addLogRequest(AddLogRequest request) {
    return null;
  }

  @Override
  public RaftRpcResponest voteRequest(VoteRequest request) {
    return null;
  }

  @Override
  public DataResponest getData(GetData request) {
    return null;
  }

  @Override
  public DataResponest setData(String request) {
    return null;
  }

  @Override
  public DataResponest doDataExchange() {
    return null;
  }

  @Override
  public DataResponest opentransaction(String request) {
    return new DataResponest(StatusCode.NON_SEVICE, "Learner 角色不支持事务操作");
  }

  @Override
  public DataResponest commitTransaction(String request) {
    return new DataResponest(StatusCode.NON_SEVICE, "Learner 角色不支持事务操作");
  }

  @Override
  public DataResponest rollbackTransaction(String request) {
    return new DataResponest(StatusCode.NON_SEVICE, "Learner 角色不支持事务操作");
  }

  @Override
  public DataResponest putInTransaction(String request) {
    return new DataResponest(StatusCode.NON_SEVICE, "Learner 角色不支持事务操作");
  }

  @Override
  public DataResponest getInTransaction(String request) {
    return new DataResponest(StatusCode.NON_SEVICE, "Learner 角色不支持事务操作");
  }

  @Override
  public DataResponest deleteInTransaction(String request) {
    return new DataResponest(StatusCode.NON_SEVICE, "Learner 角色不支持事务操作");
  }

  @Override
  public DataResponest leaderMove(com.zhiyuan.zm.raft.dto.LeaderMoveDto leaderMoveDto) {
    return new DataResponest(StatusCode.NON_SEVICE, "Learner 角色不支持 leader 漂移");
  }

  @Override
  public DataResponest snapshaotCopy(String request) {
    return null;
  }

  @Override
  public DataResponest getRaftInfo() {
    return null;
  }

  @Override
  public DataResponest configurationChange(com.zhiyuan.zm.raft.dto.ConfigurationChangeDto configurationChangeDto) {
    return null;
  }
}
