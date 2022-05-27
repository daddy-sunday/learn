package org.example;

import org.example.raft.constant.MessageType;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.VoteRequest;
import org.example.raft.rpc.DefaultRpcClient;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alipay.remoting.exception.RemotingException;

/**
 *@author zhouzhiyuan
 *@date 2022/5/23
 */
public class RpcTest {

  public static Integer a = 10;
  @Test
  public void Clint() throws RemotingException, InterruptedException {

    RaftRpcResponest raftRpcResponest = DefaultRpcClient
        .sendMessage("localhost:20000", new RaftRpcRequest(MessageType.VOTE, JSON.toJSONString(new VoteRequest())),
            30000);
    System.out.println(raftRpcResponest.toString());

  }
}
