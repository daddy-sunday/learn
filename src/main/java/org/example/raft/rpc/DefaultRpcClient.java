package org.example.raft.rpc;

import org.example.raft.dto.DataRequest;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.RaftRpcResponest;

import com.alipay.remoting.exception.RemotingException;
import com.alipay.remoting.rpc.RpcClient;

/**
 * 将客户端做成静态 ，未知的危险：不知道客户端是否支持并发使用，每个请求中都会使用到客户端
 *@author zhouzhiyuan
 *@date 2021/10/21
 */
public class DefaultRpcClient {

  private static final RpcClient client =  new RpcClient();;

  private static int defaultTimeout = 1000;

  static {
    client.init();
  }


  public static void close(String url){
    client.closeConnection(url);
  }

  public static DataResponest  dataRequest(String url, DataRequest request) throws RemotingException, InterruptedException {
    return  (DataResponest)client.invokeSync(url,request, defaultTimeout);
  }

  public DataResponest dataRequest(String url, DataRequest request,int timeOut) throws RemotingException, InterruptedException {
    return  (DataResponest)client.invokeSync(url,request,timeOut);
  }

  public static RaftRpcResponest sendMessage(String url, RaftRpcRequest request) throws RemotingException, InterruptedException {
    return  (RaftRpcResponest)client.invokeSync(url,request, defaultTimeout);
  }

  public static RaftRpcResponest sendMessage(String url, RaftRpcRequest request, int timeOut) throws RemotingException, InterruptedException {
    return  (RaftRpcResponest)client.invokeSync(url,request,timeOut);
  }

  public static int getDefaultTimeout() {
    return defaultTimeout;
  }

  public static void setDefaultTimeout(int defaultTimeout) {
    DefaultRpcClient.defaultTimeout = defaultTimeout;
  }
}