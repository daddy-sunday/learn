package com.zhiyuan.zm.raft.dto.monitor;

import java.util.List;

/**
 * 节点详细指标 DTO
 * @author zhouzhiyuan
 * @date 2026/03/06
 */
public class MetricsDTO {

    /**
     * 节点地址
     */
    private String address;

    /**
     * 同步日志队列大小
     */
    private int synLogQueueSize;

    /**
     * 应用日志队列大小
     */
    private int applyLogQueueSize;

    /**
     * 保存日志队列大小
     */
    private int saveLogQueueSize;

    /**
     * 有效成员队列
     */
    private List<String> validMembers;

    /**
     * 失败成员队列
     */
    private List<FailedMemberDTO> failedMembers;

    /**
     * Leader 繁忙状态
     */
    private long busynessStatus;

    /**
     * 租约到期时间
     */
    private long leaseEndTime;

    /**
     * 当前日志索引（Leader）
     */
    private long logIndex;

    public MetricsDTO() {
    }

    public MetricsDTO(String address, int synLogQueueSize, int applyLogQueueSize,
                     int saveLogQueueSize, List<String> validMembers,
                     List<FailedMemberDTO> failedMembers, long busynessStatus,
                     long leaseEndTime, long logIndex) {
        this.address = address;
        this.synLogQueueSize = synLogQueueSize;
        this.applyLogQueueSize = applyLogQueueSize;
        this.saveLogQueueSize = saveLogQueueSize;
        this.validMembers = validMembers;
        this.failedMembers = failedMembers;
        this.busynessStatus = busynessStatus;
        this.leaseEndTime = leaseEndTime;
        this.logIndex = logIndex;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public int getSynLogQueueSize() {
        return synLogQueueSize;
    }

    public void setSynLogQueueSize(int synLogQueueSize) {
        this.synLogQueueSize = synLogQueueSize;
    }

    public int getApplyLogQueueSize() {
        return applyLogQueueSize;
    }

    public void setApplyLogQueueSize(int applyLogQueueSize) {
        this.applyLogQueueSize = applyLogQueueSize;
    }

    public int getSaveLogQueueSize() {
        return saveLogQueueSize;
    }

    public void setSaveLogQueueSize(int saveLogQueueSize) {
        this.saveLogQueueSize = saveLogQueueSize;
    }

    public List<String> getValidMembers() {
        return validMembers;
    }

    public void setValidMembers(List<String> validMembers) {
        this.validMembers = validMembers;
    }

    public List<FailedMemberDTO> getFailedMembers() {
        return failedMembers;
    }

    public void setFailedMembers(List<FailedMemberDTO> failedMembers) {
        this.failedMembers = failedMembers;
    }

    public long getBusynessStatus() {
        return busynessStatus;
    }

    public void setBusynessStatus(long busynessStatus) {
        this.busynessStatus = busynessStatus;
    }

    public long getLeaseEndTime() {
        return leaseEndTime;
    }

    public void setLeaseEndTime(long leaseEndTime) {
        this.leaseEndTime = leaseEndTime;
    }

    public long getLogIndex() {
        return logIndex;
    }

    public void setLogIndex(long logIndex) {
        this.logIndex = logIndex;
    }

    /**
     * 失败成员 DTO
     */
    public static class FailedMemberDTO {
        private String address;
        private int groupId;
        private long needSyncLogIndex;

        public FailedMemberDTO() {
        }

        public FailedMemberDTO(String address, int groupId, long needSyncLogIndex) {
            this.address = address;
            this.groupId = groupId;
            this.needSyncLogIndex = needSyncLogIndex;
        }

        public String getAddress() {
            return address;
        }

        public void setAddress(String address) {
            this.address = address;
        }

        public int getGroupId() {
            return groupId;
        }

        public void setGroupId(int groupId) {
            this.groupId = groupId;
        }

        public long getNeedSyncLogIndex() {
            return needSyncLogIndex;
        }

        public void setNeedSyncLogIndex(long needSyncLogIndex) {
            this.needSyncLogIndex = needSyncLogIndex;
        }
    }
}
