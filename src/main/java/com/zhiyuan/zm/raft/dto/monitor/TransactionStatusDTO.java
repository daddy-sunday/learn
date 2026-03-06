package com.zhiyuan.zm.raft.dto.monitor;

import java.util.List;
import java.util.Map;

/**
 * 事务状态 DTO
 * @author zhouzhiyuan
 * @date 2026/03/06
 */
public class TransactionStatusDTO {

    /**
     * 活跃事务数
     */
    private int activeTransactionCount;

    /**
     * 已分配的事务 ID
     */
    private long allocatedTransactionId;

    /**
     * 最大事务 ID
     */
    private long maxTransactionId;

    /**
     * 事务状态分布
     */
    private TransactionDistributionDTO distribution;

    /**
     * 活跃事务列表
     */
    private List<ActiveTransactionDTO> activeTransactions;

    public TransactionStatusDTO() {
    }

    public TransactionStatusDTO(int activeTransactionCount, long allocatedTransactionId,
                               long maxTransactionId, TransactionDistributionDTO distribution,
                               List<ActiveTransactionDTO> activeTransactions) {
        this.activeTransactionCount = activeTransactionCount;
        this.allocatedTransactionId = allocatedTransactionId;
        this.maxTransactionId = maxTransactionId;
        this.distribution = distribution;
        this.activeTransactions = activeTransactions;
    }

    public int getActiveTransactionCount() {
        return activeTransactionCount;
    }

    public void setActiveTransactionCount(int activeTransactionCount) {
        this.activeTransactionCount = activeTransactionCount;
    }

    public long getAllocatedTransactionId() {
        return allocatedTransactionId;
    }

    public void setAllocatedTransactionId(long allocatedTransactionId) {
        this.allocatedTransactionId = allocatedTransactionId;
    }

    public long getMaxTransactionId() {
        return maxTransactionId;
    }

    public void setMaxTransactionId(long maxTransactionId) {
        this.maxTransactionId = maxTransactionId;
    }

    public TransactionDistributionDTO getDistribution() {
        return distribution;
    }

    public void setDistribution(TransactionDistributionDTO distribution) {
        this.distribution = distribution;
    }

    public List<ActiveTransactionDTO> getActiveTransactions() {
        return activeTransactions;
    }

    public void setActiveTransactions(List<ActiveTransactionDTO> activeTransactions) {
        this.activeTransactions = activeTransactions;
    }

    /**
     * 事务状态分布
     */
    public static class TransactionDistributionDTO {
        private int openCount;
        private int closeCount;
        private int rollbackCount;

        public TransactionDistributionDTO() {
        }

        public TransactionDistributionDTO(int openCount, int closeCount, int rollbackCount) {
            this.openCount = openCount;
            this.closeCount = closeCount;
            this.rollbackCount = rollbackCount;
        }

        public int getOpenCount() {
            return openCount;
        }

        public void setOpenCount(int openCount) {
            this.openCount = openCount;
        }

        public int getCloseCount() {
            return closeCount;
        }

        public void setCloseCount(int closeCount) {
            this.closeCount = closeCount;
        }

        public int getRollbackCount() {
            return rollbackCount;
        }

        public void setRollbackCount(int rollbackCount) {
            this.rollbackCount = rollbackCount;
        }
    }

    /**
     * 活跃事务 DTO
     */
    public static class ActiveTransactionDTO {
        private String clientId;
        private long transactionId;
        private String status;
        private long timestamp;
        private long dataCount;
        private Long snapshotTs;
        private Long beginTs;
        private java.util.Set<String> writeSet;

        public ActiveTransactionDTO() {
        }

        public ActiveTransactionDTO(String clientId, long transactionId, String status,
                                   long timestamp, long dataCount, Long snapshotTs,
                                   Long beginTs, java.util.Set<String> writeSet) {
            this.clientId = clientId;
            this.transactionId = transactionId;
            this.status = status;
            this.timestamp = timestamp;
            this.dataCount = dataCount;
            this.snapshotTs = snapshotTs;
            this.beginTs = beginTs;
            this.writeSet = writeSet;
        }

        public String getClientId() {
            return clientId;
        }

        public void setClientId(String clientId) {
            this.clientId = clientId;
        }

        public long getTransactionId() {
            return transactionId;
        }

        public void setTransactionId(long transactionId) {
            this.transactionId = transactionId;
        }

        public String getStatus() {
            return status;
        }

        public void setStatus(String status) {
            this.status = status;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public void setTimestamp(long timestamp) {
            this.timestamp = timestamp;
        }

        public long getDataCount() {
            return dataCount;
        }

        public void setDataCount(long dataCount) {
            this.dataCount = dataCount;
        }

        public Long getSnapshotTs() {
            return snapshotTs;
        }

        public void setSnapshotTs(Long snapshotTs) {
            this.snapshotTs = snapshotTs;
        }

        public Long getBeginTs() {
            return beginTs;
        }

        public void setBeginTs(Long beginTs) {
            this.beginTs = beginTs;
        }

        public java.util.Set<String> getWriteSet() {
            return writeSet;
        }

        public void setWriteSet(java.util.Set<String> writeSet) {
            this.writeSet = writeSet;
        }
    }
}
