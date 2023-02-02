package com.zhiyuan.zm.raft.rpc;

import com.zhiyuan.zm.conf.GlobalConfig;

import com.alipay.remoting.rpc.RpcServer;
import com.alipay.remoting.rpc.protocol.UserProcessor;

/**
 *@author zhouzhiyuan
 *@date 2021/10/21
 */
public class DefaultRpcServer {

  private RpcServer rpcServer ;

  public DefaultRpcServer(GlobalConfig config, UserProcessor... userProcessors) {
    this.rpcServer = new RpcServer(config.getPort());
    for (UserProcessor userProcessor : userProcessors) {
      rpcServer.registerUserProcessor(userProcessor);
    }
  }

  public Boolean start(){
    return rpcServer.start();
  }

  public void stop(){
    rpcServer.stop();
  }


}
