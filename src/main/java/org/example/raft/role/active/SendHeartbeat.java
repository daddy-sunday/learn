package org.example.raft.role.active;

import java.util.concurrent.Callable;

import org.example.raft.constant.StatusCode;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.SynchronizeLogResult;
import org.example.raft.role.RoleStatus;
import org.example.raft.rpc.DefaultRpcClient;
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

  private long term;

  public SendHeartbeat(RoleStatus roleStatus, RaftRpcRequest raftRpcRequest, String sendAddress, long term) {
    this.roleStatus = roleStatus;
    this.raftRpcRequest = raftRpcRequest;
    this.sendAddress = sendAddress;
    this.term = term;
  }

  /**
   * 发送心跳，当返回的term大于自己时转变为 follower
   * todo 重试机制
   */
  @Override
  public SynchronizeLogResult call() {
    SynchronizeLogResult result;
    try {
      RaftRpcResponest raftRpcResponest = DefaultRpcClient.sendMessage(sendAddress, raftRpcRequest);
      if (raftRpcResponest.getTerm() > term) {
        roleStatus.leaderToFollower();
        result = new SynchronizeLogResult(sendAddress, raftRpcResponest.getStatus(),StatusCode.MIN_TERM);
        LOG.info("leader->send heartbeat : receive term > current term ,leader to follower");
      }else {
        result = new SynchronizeLogResult(sendAddress, raftRpcResponest.getStatus(),StatusCode.NOT_MATCH_LOG_INDEX);
      }
    } catch (Exception e) {
      LOG.warn("leader->send heartbeat :" + e.getMessage() + " address: " + sendAddress);
      result =  new SynchronizeLogResult(sendAddress, false, StatusCode.EXCEPTION);
    }
    return result;
  }
}
