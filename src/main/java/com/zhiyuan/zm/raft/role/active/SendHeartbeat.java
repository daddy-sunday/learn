package com.zhiyuan.zm.raft.role.active;

import java.util.concurrent.Callable;

import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.RaftRpcRequest;
import com.zhiyuan.zm.raft.dto.RaftRpcResponest;
import com.zhiyuan.zm.raft.dto.SynchronizeLogResult;
import com.zhiyuan.zm.raft.role.RoleStatus;
import com.zhiyuan.zm.raft.rpc.InternalRpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *@author zhouzhiyuan
 *@date 2021/11/24
 */
public class SendHeartbeat implements Callable<SynchronizeLogResult> {

  private static final Logger LOG = LoggerFactory.getLogger(SendVote.class);

  private RoleStatus roleStatus;

  private RaftRpcRequest raftRpcRequest;

  private String sendAddress;

  private int timeout;

  public SendHeartbeat(RoleStatus roleStatus, RaftRpcRequest raftRpcRequest, String sendAddress, int timeout) {
    this.roleStatus = roleStatus;
    this.raftRpcRequest = raftRpcRequest;
    this.sendAddress = sendAddress;
    this.timeout = timeout;
  }

  /**
   * 发送心跳，当返回的term大于自己时转变为 follower
   * todo 重试机制
   */
  @Override
  public SynchronizeLogResult call() {
    SynchronizeLogResult result;
    try {
      RaftRpcResponest raftRpcResponest = InternalRpcClient.sendMessage(sendAddress, raftRpcRequest,timeout);
      if (raftRpcResponest.getFailCause() == StatusCode.MIN_TERM) {
        roleStatus.leaderToFollower();
        LOG.info("leader->send heartbeat : receive term > current term ,leader to follower");
      }
      result = new SynchronizeLogResult(sendAddress, raftRpcResponest.getStatus(), raftRpcResponest.getFailCause());
    } catch (Exception e) {
      LOG.warn("leader->send heartbeat :" + e.getMessage() + " address: " + sendAddress);
      result =  new SynchronizeLogResult(sendAddress, false, StatusCode.EXCEPTION);
    }
    return result;
  }
}
