package com.zhiyuan.zm.raft.dto;

/**
 * @author zhouzhiyuan
 * @date 2022/11/21 15:27
 */
public class LeaderMoveDto {

  private String newLeaderAddress;

  private String oldLeaderAddress;

  public LeaderMoveDto(String newLeaderAddress, String oldLeaderAddress) {
    this.newLeaderAddress = newLeaderAddress;
    this.oldLeaderAddress = oldLeaderAddress;
  }

  public LeaderMoveDto() {
  }

  public String getOldLeaderAddress() {
    return oldLeaderAddress;
  }

  public void setOldLeaderAddress(String oldLeaderAddress) {
    this.oldLeaderAddress = oldLeaderAddress;
  }

  public String getNewLeaderAddress() {
    return newLeaderAddress;
  }

  public void setNewLeaderAddress(String newLeaderAddress) {
    this.newLeaderAddress = newLeaderAddress;
  }
}
