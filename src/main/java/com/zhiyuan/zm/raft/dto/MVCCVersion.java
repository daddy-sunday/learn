package com.zhiyuan.zm.raft.dto;

/**
 * MVCC 数据版本类
 * 每个数据版本包含实际值、事务 ID、提交时间戳和状态
 *
 * @author zhouzhiyuan
 * @date 2026/03/05
 */
public class MVCCVersion {

    /**
     * 实际数据值
     */
    private byte[] value;

    /**
     * 创建该版本的事务 ID
     */
    private long transactionId;

    /**
     * 提交时间戳（用于可见性判断）
     */
    private long commitTs;

    /**
     * 版本状态：UNCOMMITTED(0), COMMITTED(1), ABORTED(2)
     */
    private byte status;

    /**
     * 删除标记（true 表示该版本已被删除）
     */
    private boolean deleted;

    public MVCCVersion() {
    }

    public MVCCVersion(byte[] value, long transactionId) {
        this.value = value;
        this.transactionId = transactionId;
        this.status = Status.UNCOMMITTED;
        this.commitTs = 0;
        this.deleted = false;
    }

    public MVCCVersion(byte[] value, long transactionId, long commitTs, byte status) {
        this.value = value;
        this.transactionId = transactionId;
        this.commitTs = commitTs;
        this.status = status;
        this.deleted = false;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    public long getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
    }

    public long getCommitTs() {
        return commitTs;
    }

    public void setCommitTs(long commitTs) {
        this.commitTs = commitTs;
    }

    public byte getStatus() {
        return status;
    }

    public void setStatus(byte status) {
        this.status = status;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public boolean isCommitted() {
        return this.status == Status.COMMITTED;
    }

    public boolean isUncommitted() {
        return this.status == Status.UNCOMMITTED;
    }

    public boolean isAborted() {
        return this.status == Status.ABORTED;
    }

    /**
     * 版本状态常量
     */
    public static class Status {
        /**
         * 未提交状态
         */
        public static final byte UNCOMMITTED = 0;

        /**
         * 已提交状态
         */
        public static final byte COMMITTED = 1;

        /**
         * 已回滚状态
         */
        public static final byte ABORTED = 2;
    }

    @Override
    public String toString() {
        return "MVCCVersion{" +
                "value=" + (value != null ? value.length : 0) + " bytes" +
                ", transactionId=" + transactionId +
                ", commitTs=" + commitTs +
                ", status=" + status +
                ", deleted=" + deleted +
                '}';
    }
}
