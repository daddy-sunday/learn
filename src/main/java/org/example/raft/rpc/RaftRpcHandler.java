package org.example.raft.rpc;

import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.service.RoleService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alipay.remoting.BizContext;

/**
 *@author zhouzhiyuan
 *@date 2021/10/21
 */
public class RaftRpcHandler extends RpcHandle {

  private final static Logger LOG = LoggerFactory.getLogger(RaftRpcHandler.class);

  private RoleService roleService;


  public RaftRpcHandler(RoleService roleService){
    this.roleService = roleService;
  }


  @Override
  public Object handleRequest(BizContext bizContext, RaftRpcRequest request) throws Exception {
    return roleService.processRaftRequest(request);
  }


}
