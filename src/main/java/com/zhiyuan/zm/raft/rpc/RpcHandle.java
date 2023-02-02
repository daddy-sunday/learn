package com.zhiyuan.zm.raft.rpc;

import com.zhiyuan.zm.raft.dto.RaftRpcRequest;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AbstractUserProcessor;

/**
 *@author zhouzhiyuan
 *@date 2021/10/21
 */
public abstract class RpcHandle extends AbstractUserProcessor<RaftRpcRequest> {


  @Override
  public void handleRequest(BizContext bizContext, AsyncContext asyncContext, RaftRpcRequest request) {
   throw  new RuntimeException("暂不支持的功能 异步消息传递");
  }
  @Override
  public String interest() {
    return  RaftRpcRequest.class.getName();
  }
}
