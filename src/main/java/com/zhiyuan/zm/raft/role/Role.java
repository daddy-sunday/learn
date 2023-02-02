package com.zhiyuan.zm.raft.role;

import com.zhiyuan.zm.raft.dto.AddLogRequest;
import com.zhiyuan.zm.raft.dto.ConfigurationChangeDto;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.GetData;
import com.zhiyuan.zm.raft.dto.LeaderMoveDto;
import com.zhiyuan.zm.raft.dto.RaftRpcResponest;
import com.zhiyuan.zm.raft.dto.VoteRequest;

/**
 * 在其位谋其职
 *@author zhouzhiyuan
 *@date 2021/10/27
 */
public interface Role {

  /**
   * 不同的角色做不同的事情
   */
  void  work();

  /**
   * 响应追加log请求
   * @return xx
   * @param request
   */
  RaftRpcResponest addLogRequest(AddLogRequest request);

  /**
   *响应选举请求
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
  DataResponest setData(String request);


  /**
   * 角色之间的状态交换，比如follower想知道当前leader的 log entires index 以支持读取数据。
   * @param request 1
   * @return 1
   */
  DataResponest dataExchange(String request);


  /**
   * 配置变更
   * @param configurationChangeDto 1
   * @return 1
   */
  DataResponest configurationChange(ConfigurationChangeDto configurationChangeDto);

  /**
   * leader 漂移
   * @param leaderMoveDto =
   * @return =
   */
  DataResponest leaderMove(LeaderMoveDto leaderMoveDto);

  /**
   *
   * @param request =
   * @return =
   */
  DataResponest snapshaotCopy(String request);


  DataResponest getRaftInfo();


}
