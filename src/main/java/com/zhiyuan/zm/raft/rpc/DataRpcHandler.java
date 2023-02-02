package com.zhiyuan.zm.raft.rpc;

import com.zhiyuan.zm.raft.dto.DataRequest;
import com.zhiyuan.zm.raft.service.RoleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.AsyncContext;
import com.alipay.remoting.BizContext;
import com.alipay.remoting.rpc.protocol.AbstractUserProcessor;

/**
 *@author zhouzhiyuan
 *@date 2021/11/23
 */
public class DataRpcHandler extends AbstractUserProcessor<DataRequest> {

  private final static Logger LOG = LoggerFactory.getLogger(RaftRpcHandler.class);

  private RoleService roleService;


  public DataRpcHandler(RoleService roleService){
    this.roleService = roleService;
  }

  @Override
  public void handleRequest(BizContext bizCtx, AsyncContext asyncCtx, DataRequest request) {
    throw  new RuntimeException("暂不支持的功能 异步消息传递");
  }

  @Override
  public Object handleRequest(BizContext bizContext, DataRequest request) throws Exception {
    LOG.debug("当前线程  "+Thread.currentThread().getName()+"  接受到 "+request+" 的请求");
    return roleService.processDataRequest(request);
  }

  @Override
  public String interest() {
    return DataRequest.class.getName();
  }
}
