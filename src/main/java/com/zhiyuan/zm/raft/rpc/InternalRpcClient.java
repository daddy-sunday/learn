package com.zhiyuan.zm.raft.rpc;

import com.zhiyuan.zm.raft.constant.MessageType;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.DataChangeDto;
import com.zhiyuan.zm.raft.dto.DataRequest;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.LeaderMoveDto;
import com.zhiyuan.zm.raft.dto.RaftRpcRequest;
import com.zhiyuan.zm.raft.dto.RaftRpcResponest;

import com.alibaba.fastjson.JSON;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;

/**
 * 这个客户端是内部使用的，用户不能使用
 *@author zhouzhiyuan
 *@date 2021/10/21
 */
public class InternalRpcClient {

  private static final RpcClient client =  new RpcClient();;

  private static int defaultTimeout = 10000;

  static {
    client.init();
  }

  public static void close(String url) {
    client.closeConnection(url);
  }

  public static DataChangeDto dataChange(String url, int timeout, int role)
      throws RemotingException, InterruptedException {
    DataResponest result = dataRequest(url, new DataRequest(MessageType.READ_INDEX, Integer.toString(role)),timeout);
    if (result.getStatus() == StatusCode.SUCCESS) {
      return JSON.parseObject(result.getMessage(), DataChangeDto.class);
    }
    throw new RemotingException(result.getMessage());
  }

  public static DataResponest followerToLeader(String url, int timeout, LeaderMoveDto leaderMoveDto)
      throws RemotingException, InterruptedException {
    return dataRequest(url, new DataRequest(MessageType.LEADER_MOVE, JSON.toJSONString(leaderMoveDto)),timeout);
  }


  private static DataResponest dataRequest(String url, DataRequest request, int timeOut)
      throws RemotingException, InterruptedException {
    return (DataResponest) client.invokeSync(url, request, timeOut);
  }

  public static RaftRpcResponest sendMessage(String url, RaftRpcRequest request, int timeOut)
      throws RemotingException, InterruptedException {
    return (RaftRpcResponest) client.invokeSync(url, request, timeOut);
  }
}
