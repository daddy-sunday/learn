package org.example.raft.role;

import org.example.raft.dto.AddLog;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.GetData;
import org.example.raft.dto.LogEntry;
import org.example.raft.dto.RaftRpcResponest;
import org.example.raft.dto.VoteRequest;

/**
 * 在其位谋其职
 *@author zhouzhiyuan
 *@date 2021/10/27
 */
public interface Role {

  /**
   * 不同的角色做不同的事情
   */
  void work();

  /**
   * 响应追加log请求
   * @return xx
   * @param request
   */
  RaftRpcResponest addLogRequest(AddLog request);

  /**
   *响应选举log请求
   * @return xx
   * @param request
   */
  RaftRpcResponest voteRequest(VoteRequest request);

  /**
   *获取数据接口
   * @param request
   * @return
   */
  DataResponest getData(GetData request);

  /**
   *存储数据接口
   * @param request
   * @return
   */
  DataResponest setData(LogEntry[] request);


}