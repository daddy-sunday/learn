package com.zhiyuan.zm.raft.dto;

/**
 * @author zhouzhiyuan
 * @date 2022/11/24 11:31
 */
public class RaftInfoDto {
  private String leaderAddress;

  public RaftInfoDto() {
  }

  public RaftInfoDto(String leaderAddress) {
    this.leaderAddress = leaderAddress;
  }

  public String getLeaderAddress() {
    return leaderAddress;
  }

  public void setLeaderAddress(String leaderAddress) {
    this.leaderAddress = leaderAddress;
  }
}
