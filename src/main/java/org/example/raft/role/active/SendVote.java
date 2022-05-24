package org.example.raft.role.active;

import java.util.concurrent.Callable;

import org.example.raft.constant.MessageType;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.VoteRequest;
import org.example.raft.rpc.DefaultRpcClient;
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
       raftRpcResponest = DefaultRpcClient
          .sendMessage(sendAddress, new RaftRpcRequest(MessageType.VOTE, JSON.toJSONString(voteRequest)),300);
    }catch (Exception e){
      LOG.warn("candidate-> send vote  failed: "+e.getMessage()+" address: "+sendAddress);
    }

    return raftRpcResponest != null && raftRpcResponest.getStatus();
  }
}
