package com.zhiyuan.zm.raft.role.active;

import java.util.concurrent.Callable;

import com.zhiyuan.zm.raft.constant.MessageType;
import com.zhiyuan.zm.raft.dto.RaftRpcRequest;
import com.zhiyuan.zm.raft.dto.RaftRpcResponest;
import com.zhiyuan.zm.raft.dto.VoteRequest;
import com.zhiyuan.zm.raft.rpc.InternalRpcClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 *@author zhouzhiyuan
 *@date 2021/11/1
 */
public class SendVote implements Callable<Boolean> {

  private static final Logger LOG = LoggerFactory.getLogger(SendVote.class);

  private VoteRequest voteRequest;

  private String sendAddress;


  public SendVote(VoteRequest voteRequest, String sendAddress) {
    this.voteRequest = voteRequest;
    this.sendAddress = sendAddress;
  }

  @Override
  public Boolean call(){
    RaftRpcResponest raftRpcResponest = null;
    try{
       raftRpcResponest = InternalRpcClient
          .sendMessage(sendAddress, new RaftRpcRequest(MessageType.VOTE, JSON.toJSONString(voteRequest)),300);
    }catch (Exception e){
      LOG.warn("candidate-> send vote  failed: "+e.getMessage()+" address: "+sendAddress);
    }

    return raftRpcResponest != null && raftRpcResponest.getStatus();
  }
}
