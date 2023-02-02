package com.zhiyuan.zm.raft.rpc;

import com.alibaba.fastjson.JSON;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;
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

/**
 * @author zhouzhiyuan
 * @date 2023/2/1 18:32
 */
public class ZMClient {

  private  final RpcClient client ;

  private  int timeOut = 10000;

  /**
   * 这个值在运行过程中会随着leader节点的变更而变更
   */
  private String url;

  public ZMClient(String url) {
    this.client = new RpcClient();
    client.init();
    this.url = url;
  }

  public ZMClient(int defaultTimeout, String url) {
    this.client = new RpcClient();
    client.init();
    this.timeOut = defaultTimeout;
    this.url = url;
  }

  public  void close(String url) {
    client.closeConnection(url);
  }

  public  DataResponest leaderMove(String newLeader) throws RemotingException, InterruptedException {
    RaftInfoDto raftInfo = getRaftInfo(url, timeOut);
    LeaderMoveDto leaderMoveDto = new LeaderMoveDto(newLeader,raftInfo.getLeaderAddress());
    DataRequest request = new DataRequest(MessageType.LEADER_MOVE,
        JSON.toJSONString(leaderMoveDto));
    return dataRequest(raftInfo.getLeaderAddress(), request, timeOut);
  }

  private  RaftInfoDto getRaftInfo(String url,int timeOut) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.RAFT_INFO,null);
    DataResponest dataResponest = dataRequest(url, request, timeOut);

    if (dataResponest.getStatus() == StatusCode.SUCCESS) {
      return JSON.parseObject(dataResponest.getMessage(), RaftInfoDto.class);
    }
    throw new RemotingException(dataResponest.getMessage());
  }


  public  DataResponest put(Row[] data) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.SET,
        JSON.toJSONString(new Command(DataOperationType.INSERT, data)));
    DataResponest dataResponest = dataRequest(url, request, timeOut);
    if (dataResponest.getStatus() == StatusCode.REDIRECT) {
      url = dataResponest.getMessage();
      return dataRequest(url, request, timeOut);
    }
    return dataResponest;
  }

  public  DataResponest delete( Row[] data)
      throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.SET,
        JSON.toJSONString(new Command(DataOperationType.DELETE, data)));
    DataResponest dataResponest = dataRequest(url, request, timeOut);
    if (dataResponest.getStatus() == StatusCode.REDIRECT) {
      url = dataResponest.getMessage();
      return dataRequest(url, request, timeOut);
    }
    return dataResponest;
  }

  public  DataResponest get( String key) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.GET,
        JSON.toJSONString(new GetData(key)));
    return dataRequest(url, request, timeOut);
  }


  private  DataResponest dataRequest(String url, DataRequest request, int timeOut)
      throws RemotingException, InterruptedException {
    return (DataResponest) client.invokeSync(url, request, timeOut);
  }
}
