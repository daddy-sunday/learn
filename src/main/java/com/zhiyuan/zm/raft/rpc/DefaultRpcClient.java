package com.zhiyuan.zm.raft.rpc;

import com.zhiyuan.zm.raft.constant.DataOperationType;
import com.zhiyuan.zm.raft.constant.MessageType;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.Command;
import com.zhiyuan.zm.raft.dto.DataRequest;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.GetData;
import com.zhiyuan.zm.raft.dto.LeaderMoveDto;
import com.zhiyuan.zm.raft.dto.RaftInfoDto;
import com.zhiyuan.zm.raft.dto.Row;

import com.alibaba.fastjson.JSON;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;

/**
 * 这个客户端是对外的客户端
 *@author zhouzhiyuan
 *@date 2021/10/21
 */
public class DefaultRpcClient {

  private static final RpcClient client =  new RpcClient();

  private static int defaultTimeout = 10000;

  static {
    client.init();
  }

  public static void close(String url) {
    client.closeConnection(url);
  }

  private static DataResponest dataRequest(String url, DataRequest request)
      throws RemotingException, InterruptedException {
    return (DataResponest) client.invokeSync(url, request, defaultTimeout);
  }

  public static DataResponest leaderMove(String url,String newLeader, int timeOut) throws RemotingException, InterruptedException {
    RaftInfoDto raftInfo = getRaftInfo(url, timeOut);
    LeaderMoveDto leaderMoveDto = new LeaderMoveDto(newLeader,raftInfo.getLeaderAddress());
    DataRequest request = new DataRequest(MessageType.LEADER_MOVE,
        JSON.toJSONString(leaderMoveDto));
    return dataRequest(raftInfo.getLeaderAddress(), request, timeOut);
  }

  public static RaftInfoDto getRaftInfo(String url,int timeOut) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.RAFT_INFO,null);
    DataResponest dataResponest = dataRequest(url, request, timeOut);

    if (dataResponest.getStatus() == StatusCode.SUCCESS) {
      return JSON.parseObject(dataResponest.getMessage(), RaftInfoDto.class);
    }
    throw new RemotingException(dataResponest.getMessage());
  }


  public static DataResponest put(String url, Row[] data, int timeOut) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.SET,
        JSON.toJSONString(new Command(DataOperationType.INSERT, data)));
    DataResponest dataResponest = dataRequest(url, request, timeOut);
    if (dataResponest.getStatus() == StatusCode.REDIRECT) {
      return dataRequest(dataResponest.getMessage(), request, timeOut);
    }
    return dataResponest;
  }

  public static DataResponest delete(String url, Row[] data, int timeOut)
      throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.SET,
        JSON.toJSONString(new Command(DataOperationType.DELETE, data)));
    DataResponest dataResponest = dataRequest(url, request, timeOut);
    if (dataResponest.getStatus() == StatusCode.REDIRECT) {
      return dataRequest(dataResponest.getMessage(), request, timeOut);
    }
    return dataResponest;
  }

  public static DataResponest get(String url, String key, int timeOut) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.GET,
        JSON.toJSONString(new GetData(key)));
    return dataRequest(url, request, timeOut);
  }


  private static DataResponest dataRequest(String url, DataRequest request, int timeOut)
      throws RemotingException, InterruptedException {
    return (DataResponest) client.invokeSync(url, request, timeOut);
  }

}
