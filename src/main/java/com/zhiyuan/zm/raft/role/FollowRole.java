package com.zhiyuan.zm.raft.role;

import java.util.concurrent.BlockingQueue;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.constant.ServiceStatus;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.AddLogRequest;
import com.zhiyuan.zm.raft.dto.DataChangeDto;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.GetData;
import com.zhiyuan.zm.raft.dto.LeaderMoveDto;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.RaftInfoDto;
import com.zhiyuan.zm.raft.dto.RaftRpcResponest;
import com.zhiyuan.zm.raft.dto.TaskMaterial;
import com.zhiyuan.zm.raft.dto.VoteRequest;
import com.zhiyuan.zm.raft.persistence.SaveData;
import com.zhiyuan.zm.raft.persistence.SaveLog;
import com.zhiyuan.zm.raft.role.active.SaveLogTask;
import com.zhiyuan.zm.raft.rpc.InternalRpcClient;
import com.zhiyuan.zm.raft.service.RaftStatus;
import com.zhiyuan.zm.raft.util.RaftUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alipay.remoting.util.StringUtils;

/**
 * @author zhouzhiyuan
 * @date 2021/10/27
 */
public class FollowRole extends BaseRole implements Role {

  private static final Logger LOG = LoggerFactory.getLogger(FollowRole.class);

  private volatile boolean keepRuning;


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
    LogEntries maxLog = saveLog.getMaxLog(RaftUtil.generateLogKey(raftStatus.getGroupId(), Long.MAX_VALUE));
    //接收log日志时判断日志是否连续使用
    raftStatus.setLastTimeLogIndex(maxLog.getLogIndex());
    raftStatus.setLastTimeTerm(maxLog.getTerm());
    //判断超时选举用
    raftStatus.setLastTime();
    keepRuning = true;
    raftStatus.setServiceStatus(ServiceStatus.IN_SERVICE);

    LOG.info("followRole初始化完成");
  }

  @Override
  public void work() {
    init();

    do {
      try {
        LOG.debug("check  heartbeat  ");
        long l = System.currentTimeMillis();
        if (raftStatus.getLastUpdateTime() + checkTimeoutInterval < l) {
          //超时 将当前角色转换为 candidate
          roleStatus.followerToCandidate();
          LOG.info("heartbeat timeout  follower role transition candidate role");
        }
        if (keepRuning) {
          Thread.sleep(checkTimeoutInterval);
        } else {
          break;
        }
      } catch (InterruptedException e) {
        LOG.error(e.getMessage(), e);
      }
    } while (roleStatus.getNodeStatus() == RoleStatus.FOLLOWER);
    raftStatus.setServiceStatus(ServiceStatus.IN_SWITCH_ROLE);
  }

  @Override
  public RaftRpcResponest addLogRequest(AddLogRequest request) {
    inServiceWait();
    return addLogProcess(request);
  }

  /**
   * follow 状态不参与选举
   *
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
      return new DataResponest(StatusCode.NON_SEVICE,
          "服务正在初始化，请换一个节点或者等以后儿重试，状态：" + raftStatus.getServiceStatus());
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
  public DataResponest getRaftInfo() {

    if (!inService()) {
      return new DataResponest(StatusCode.NON_SEVICE,
          "服务正在初始化，请换一个节点或者等以后儿重试，状态：" + raftStatus.getServiceStatus());
    }
    try {
      if (StringUtils.isEmpty(raftStatus.getLeaderAddress())) {
        return new DataResponest(StatusCode.SLEEP, "当前服务刚启动，还没有收到leader消息，请等待一会重试");
      }
      // leader能正确返回数据，代表raft service 可用
       InternalRpcClient
          .dataChange(raftStatus.getLeaderAddress(), sendHeartbeatTimeout, RoleStatus.LEADER);
      return new DataResponest(StatusCode.SUCCESS , JSON.toJSONString(new RaftInfoDto(raftStatus.getLeaderAddress())));
    } catch (Exception e) {
      LOG.error("获取leader 信息失败", e);
    }
    return new DataResponest(StatusCode.RAFT_UNABLE_SERVER,"获取raft信息失败，请检查服务器日志");
  }


  @Override
  public DataResponest setData(String request) {
    if (!inService()) {
      return new DataResponest(StatusCode.NON_SEVICE,
          "服务正在初始化，请换一个节点或者等以后儿重试，状态：" + raftStatus.getServiceStatus());
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
        JSON.toJSONString(new DataChangeDto(raftStatus.getLastTimeLogIndex(), raftStatus.getLastTimeTerm(),
            raftStatus.getAppliedIndex())));
  }

  //
  @Override
  public DataResponest leaderMove(LeaderMoveDto leaderMoveDto) {
    if (raftStatus.getLocalAddress().equals(leaderMoveDto.getNewLeaderAddress())) {
      if (roleStatus.followerToLeader()) {
        keepRuning = false;
        thread.interrupt();
        return new DataResponest(StatusCode.SUCCESS);
      } else {
        return new DataResponest(StatusCode.ERROR_REQUEST, "执行角色切换命令时时发生了角色切换");
      }
    } else {
      return new DataResponest(StatusCode.ERROR_REQUEST, "请求节点与继位leader节点不匹配");
    }
  }
}
