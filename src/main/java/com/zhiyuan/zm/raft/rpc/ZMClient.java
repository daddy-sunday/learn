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

  private final RpcClient client;

  private int timeOut = 10000;

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

  public void close(String url) {
    client.closeConnection(url);
  }

  public DataResponest leaderMove(String newLeader) throws RemotingException, InterruptedException {
    DataResponest dataResponest = getRaftInfo(url, timeOut);
    if (dataResponest.getStatus() != StatusCode.SUCCESS) {
      return dataResponest;
    }
    RaftInfoDto raftInfo = JSON.parseObject(dataResponest.getMessage(), RaftInfoDto.class);
    LeaderMoveDto leaderMoveDto = new LeaderMoveDto(newLeader, raftInfo.getLeaderAddress());
    DataRequest request = new DataRequest(MessageType.LEADER_MOVE,
        JSON.toJSONString(leaderMoveDto));
    return dataRequest(raftInfo.getLeaderAddress(), request, timeOut);
  }

  private DataResponest getRaftInfo(String url, int timeOut) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.RAFT_INFO, null);
    return dataRequest(url, request, timeOut);
  }


  public DataResponest put(Row[] data) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.SET,
        JSON.toJSONString(new Command(DataOperationType.INSERT, data)));
    DataResponest dataResponest = dataRequest(url, request, timeOut);
    if (dataResponest.getStatus() == StatusCode.REDIRECT) {
      url = dataResponest.getMessage();
      return dataRequest(url, request, timeOut);
    }
    return dataResponest;
  }

  public DataResponest delete(Row[] data)
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

  public DataResponest get(String key) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.GET,
        JSON.toJSONString(new GetData(key)));
    return dataRequest(url, request, timeOut);
  }

  // ==================== MVCC 事务相关方法 ====================

  /**
   * 开启 MVCC 事务
   * @param clientId 客户端标识（用于标识事务）
   * @return 开启结果，成功时 data 字段包含事务 ID
   */
  public DataResponest openTransaction(String clientId) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.OPEN_TRANSACTION, clientId, clientId);
    return dataRequest(url, request, timeOut);
  }

  /**
   * 提交 MVCC 事务
   * @param clientId 客户端标识（事务标识）
   * @return 提交结果
   */
  public DataResponest commitTransaction(String clientId) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.COMMIT_TRANSACTION, clientId, clientId);
    return dataRequest(url, request, timeOut);
  }

  /**
   * 回滚 MVCC 事务
   * @param clientId 客户端标识（事务标识）
   * @return 回滚结果
   */
  public DataResponest rollbackTransaction(String clientId) throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest(MessageType.ROLLBACK_TRANSACTION, clientId, clientId);
    return dataRequest(url, request, timeOut);
  }

  /**
   * MVCC 事务内写入数据
   * @param clientId 客户端标识（事务标识）
   * @param key 数据 key
   * @param value 数据 value
   * @return 操作结果
   */
  public DataResponest putInTransaction(String clientId, String key, String value)
      throws RemotingException, InterruptedException {
    String request = JSON.toJSONString(new Command(DataOperationType.MVCC_PUT,
        new Row[]{new Row(key.getBytes(), value.getBytes())}));
    DataRequest dataRequest = new DataRequest(MessageType.PUT_IN_TRANSACTION, clientId, request);
    return dataRequest(url, dataRequest, timeOut);
  }

  /**
   * MVCC 事务内读取数据（快照读）
   * @param clientId 客户端标识（事务标识）
   * @param key 数据 key
   * @return 读取结果
   */
  public DataResponest getInTransaction(String clientId, String key)
      throws RemotingException, InterruptedException {
    String request = JSON.toJSONString(new GetData(key, clientId));
    DataRequest dataRequest = new DataRequest(MessageType.GET_IN_TRANSACTION, clientId, request);
    return dataRequest(url, dataRequest, timeOut);
  }

  /**
   * MVCC 事务内删除数据
   * @param clientId 客户端标识（事务标识）
   * @param key 数据 key
   * @return 操作结果
   */
  public DataResponest deleteInTransaction(String clientId, String key)
      throws RemotingException, InterruptedException {
    String request = JSON.toJSONString(new GetData(key, clientId));
    DataRequest dataRequest = new DataRequest(MessageType.DELETE_IN_TRANSACTION, clientId, request);
    return dataRequest(url, dataRequest, timeOut);
  }


  private DataResponest dataRequest(String url, DataRequest request, int timeOut)
      throws RemotingException, InterruptedException {
    return (DataResponest) client.invokeSync(url, request, timeOut);
  }
}
