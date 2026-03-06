package com.zhiyuan.zm.raft.dto.monitor;

import java.util.List;

/**
 * 集群状态 DTO
 * @author zhouzhiyuan
 * @date 2026/03/06
 */
public class ClusterStatusDTO {

    /**
     * 集群健康状态
     */
    private boolean healthy;

    /**
     * 当前 Leader 地址
     */
    private String leaderAddress;

    /**
     * 当前 Term（任期）
     */
    private long currentTerm;

    /**
     * 集群成员总数
     */
    private int totalMembers;

    /**
     * 有效成员数
     */
    private int validMembers;

    /**
     * 失败成员数
     */
    private int failedMembers;

    /**
     * 成员列表
     */
    private List<String> members;

    /**
     * 集群状态描述
     */
    private String status;

    public ClusterStatusDTO() {
    }

    public ClusterStatusDTO(boolean healthy, String leaderAddress, long currentTerm,
                           int totalMembers, int validMembers, int failedMembers,
                           List<String> members, String status) {
        this.healthy = healthy;
        this.leaderAddress = leaderAddress;
        this.currentTerm = currentTerm;
        this.totalMembers = totalMembers;
        this.validMembers = validMembers;
        this.failedMembers = failedMembers;
        this.members = members;
        this.status = status;
    }

    public boolean isHealthy() {
        return healthy;
    }

    public void setHealthy(boolean healthy) {
        this.healthy = healthy;
    }

    public String getLeaderAddress() {
        return leaderAddress;
    }

    public void setLeaderAddress(String leaderAddress) {
        this.leaderAddress = leaderAddress;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    public int getTotalMembers() {
        return totalMembers;
    }

    public void setTotalMembers(int totalMembers) {
        this.totalMembers = totalMembers;
    }

    public int getValidMembers() {
        return validMembers;
    }

    public void setValidMembers(int validMembers) {
        this.validMembers = validMembers;
    }

    public int getFailedMembers() {
        return failedMembers;
    }

    public void setFailedMembers(int failedMembers) {
        this.failedMembers = failedMembers;
    }

    public List<String> getMembers() {
        return members;
    }

    public void setMembers(List<String> members) {
        this.members = members;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
