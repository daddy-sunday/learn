package org.example.raft.rpc;

import org.example.raft.constant.DataOperationType;
import org.example.raft.constant.MessageType;
import org.example.raft.constant.StatusCode;
import org.example.raft.dto.Command;
import org.example.raft.dto.DataChangeDto;
import org.example.raft.dto.DataRequest;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.GetData;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.Row;

import com.alibaba.fastjson.JSON;
import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;

/**
 * 将客户端做成静态 ，未知的危险：不知道 RpcClient 客户端是否支持并发使用，每个请求中都会使用到客户端
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
