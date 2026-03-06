package com.zhiyuan.zm.raft.dto.monitor;

/**
 * 节点状态 DTO
 * @author zhouzhiyuan
 * @date 2026/03/06
 */
public class NodeStatusDTO {

    /**
     * 节点地址
     */
    private String address;

    /**
     * 节点角色 (LEADER/FOLLOWER/CANDIDATE/LEARNER)
     */
    private String role;

    /**
     * 服务状态 (IN_SERVICE/NON_SERVICE/READ_ONLY 等)
     */
    private String serviceStatus;

    /**
     * 当前 Term（任期）
     */
    private long currentTerm;

    /**
     * 已提交索引
     */
    private long commitIndex;

    /**
     * 已应用索引
     */
    private long appliedIndex;

    /**
     * 最大日志索引
     */
    private long maxLogIndex;

    /**
     * 最后更新时间
     */
    private long lastUpdateTime;

    /**
     * 是否为本节点
     */
    private boolean isLocal;

    public NodeStatusDTO() {
    }

    public NodeStatusDTO(String address, String role, String serviceStatus,
                        long currentTerm, long commitIndex, long appliedIndex,
                        long maxLogIndex, long lastUpdateTime, boolean isLocal) {
        this.address = address;
        this.role = role;
        this.serviceStatus = serviceStatus;
        this.currentTerm = currentTerm;
        this.commitIndex = commitIndex;
        this.appliedIndex = appliedIndex;
        this.maxLogIndex = maxLogIndex;
        this.lastUpdateTime = lastUpdateTime;
        this.isLocal = isLocal;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getServiceStatus() {
        return serviceStatus;
    }

    public void setServiceStatus(String serviceStatus) {
        this.serviceStatus = serviceStatus;
    }

    public long getCurrentTerm() {
        return currentTerm;
    }

    public void setCurrentTerm(long currentTerm) {
        this.currentTerm = currentTerm;
    }

    public long getCommitIndex() {
        return commitIndex;
    }

    public void setCommitIndex(long commitIndex) {
        this.commitIndex = commitIndex;
    }

    public long getAppliedIndex() {
        return appliedIndex;
    }

    public void setAppliedIndex(long appliedIndex) {
        this.appliedIndex = appliedIndex;
    }

    public long getMaxLogIndex() {
        return maxLogIndex;
    }

    public void setMaxLogIndex(long maxLogIndex) {
        this.maxLogIndex = maxLogIndex;
    }

    public long getLastUpdateTime() {
        return lastUpdateTime;
    }

    public void setLastUpdateTime(long lastUpdateTime) {
        this.lastUpdateTime = lastUpdateTime;
    }

    public boolean isLocal() {
        return isLocal;
    }

    public void setLocal(boolean local) {
        isLocal = local;
    }
}
