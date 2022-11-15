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
import org.example.raft.rpc.InternalRpcClient;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.RaftUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alipay.remoting.util.StringUtils;

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
    LOG.info("followRole初始化开始");
    LogEntries maxLog = saveLog.getMaxLog(RaftUtil.generateLogKey(raftStatus.getGroupId(),Long.MAX_VALUE));
    //接收log日志时判断日志是否连续使用
    raftStatus.setLastTimeLogIndex(maxLog.getLogIndex());
    raftStatus.setLastTimeTerm(maxLog.getTerm());
    //判断超时选举用
    raftStatus.setLastTime();

    raftStatus.setServiceStatus(ServiceStatus.IN_SERVICE);
    LOG.info("followRole初始化完成");
  }

  @Override
  public void work() {
    init();
    do {
      try {
        Thread.sleep(checkTimeoutInterval);
        LOG.debug("check  heartbeat  ");
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
    if (!inService()) {
      return new DataResponest(StatusCode.NON_SEVICE, "服务正在初始化，请换一个节点或者等以后儿重试，状态："+raftStatus.getServiceStatus());
    }
    try {
      if (StringUtils.isEmpty(raftStatus.getLeaderAddress())) {
        return new DataResponest(StatusCode.SLEEP, "当前服务刚启动，还没有收到leader消息，请等待一会重试");
      }
      DataChangeDto dataChangeDto = InternalRpcClient
          .dataChange(raftStatus.getLeaderAddress(), sendHeartbeatTimeout, RoleStatus.LEADER);
      waitApplyIndexComplate(dataChangeDto.getCommitIndex());
      return getDataCommon(request);
    } catch (Exception e) {
      LOG.error("获取leader commitIndex 失败", e);
    }

    return new DataResponest(StatusCode.SYSTEMEXCEPTION, "系统异常：请查看系统日志");
  }


  @Override
  public DataResponest setData(String request) {
    if (!inService()) {
      return new DataResponest(StatusCode.NON_SEVICE, "服务正在初始化，请换一个节点或者等以后儿重试，状态："+raftStatus.getServiceStatus());
    }
    String leaderAddress = raftStatus.getLeaderAddress();
    if (StringUtils.isEmpty(leaderAddress)) {
      return new DataResponest(StatusCode.SLEEP, "当前服务刚启动，请等待一会重试");
    }

    return new DataResponest(StatusCode.REDIRECT, leaderAddress);
  }

  @Override
  public DataResponest doDataExchange() {
    return new DataResponest(StatusCode.SUCCESS,
        JSON.toJSONString(new DataChangeDto(raftStatus.getLastTimeLogIndex(), raftStatus.getLastTimeTerm())));
  }
}
