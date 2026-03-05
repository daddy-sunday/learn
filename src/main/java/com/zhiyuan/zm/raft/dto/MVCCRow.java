package com.zhiyuan.zm.raft.dto;

/**
 * MVCC 数据行类（扁平化存储模式）
 * 存储单个版本的数据，Key 中包含 transactionId
 *
 * @author zhouzhiyuan
 * @date 2026/03/05
 */
public class MVCCRow {

    /**
     * 用户数据的 key（不包含 transactionId）
     */
    private byte[] userKey;

    /**
     * 数据版本
     */
    private MVCCVersion version;

    public MVCCRow() {
    }

    public MVCCRow(byte[] userKey, MVCCVersion version) {
        this.userKey = userKey;
        this.version = version;
    }

    public MVCCRow(byte[] userKey, byte[] value, long transactionId) {
        this.userKey = userKey;
        this.version = new MVCCVersion(value, transactionId);
    }

    public MVCCRow(byte[] userKey, byte[] value, long transactionId, long commitTs, byte status) {
        this.userKey = userKey;
        this.version = new MVCCVersion(value, transactionId, commitTs, status);
    }

    public byte[] getUserKey() {
        return userKey;
    }

    public void setUserKey(byte[] userKey) {
        this.userKey = userKey;
    }

    public MVCCVersion getVersion() {
        return version;
    }

    public void setVersion(MVCCVersion version) {
        this.version = version;
    }

    public byte[] getValue() {
        return version != null ? version.getValue() : null;
    }

    public void setValue(byte[] value) {
        if (this.version != null) {
            this.version.setValue(value);
        } else {
            this.version = new MVCCVersion(value, 0);
        }
    }

    public long getTransactionId() {
        return version != null ? version.getTransactionId() : 0;
    }

    public void setTransactionId(long transactionId) {
        if (this.version != null) {
            this.version.setTransactionId(transactionId);
        }
    }

    public long getCommitTs() {
        return version != null ? version.getCommitTs() : 0;
    }

    public void setCommitTs(long commitTs) {
        if (this.version != null) {
            this.version.setCommitTs(commitTs);
        }
    }

    public byte getStatus() {
        return version != null ? version.getStatus() : 0;
    }

    public void setStatus(byte status) {
        if (this.version != null) {
            this.version.setStatus(status);
        }
    }

    public boolean isDeleted() {
        return version != null && version.isDeleted();
    }

    public void setDeleted(boolean deleted) {
        if (this.version != null) {
            this.version.setDeleted(deleted);
        }
    }

    @Override
    public String toString() {
        return "MVCCRow{" +
                "userKey=" + (userKey != null ? new String(userKey) : "null") +
                ", version=" + version +
                '}';
    }
}
