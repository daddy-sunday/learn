package com.zhiyuan.zm.raft.util;

import java.nio.ByteBuffer;

/**
 *直接看下面的方法注释，更容易理解前缀key
 * @author zhouzhiyuan
 *@date 2022/5/10
 */
public class KeyUtil {


  public static final long  INIT_LOG_INDEX = 5;
  public static final long  INIT_TERM = 5;
  private static final byte RAFT_INIT_FLAG_KEY = 3;

  /**
   *
   * 数据类型划分
   *
   */
  private static final byte COMMIT_LOG_KEY_PREFIX = 1;

  private static final byte APPLY_LOG_KEY_PREFIX = 2;

  private static final byte LOG_KEY_PREFIX = 10;

  private static final byte DATA_KEY_PREFIX = 20;


  private static final byte TRANSACTION_KEY_PREFIX = 30;

  private static final byte TRANSACTION_MAX_ID = 31;

  private static final byte AUTO_INCREMENT_ID_PREFIX = 40;

  /**
   * MVCC 数据 Key 前缀（扁平化存储）
   * Key 格式：1 字节类型 + 4 字节 groupId + N 字节 userKey + 8 字节 transactionId
   */
  public static final byte MVCC_DATA_KEY_PREFIX = 50;

  /**
   * MVCC 全局最后提交时间戳 Key（用于分配 snapshotTs）
   */
  private static final byte MVCC_LAST_COMMIT_TS_PREFIX = 51;


  /**
   * 事务id key
   * @return
   */
  public static byte[] generateCacheTransactionIdKey() {
    return generateCommon(Integer.MAX_VALUE, TRANSACTION_MAX_ID);
  }

  public static byte[] generateTransactionIdKey(long transactionId) {
    return generateCommon(transactionId, TRANSACTION_KEY_PREFIX);
  }


  /**
   * 是否做过初始化
   * @param raftGroupId
   * @return
   */
  public static byte[] generateRaftInitKey(int raftGroupId) {
    return generateCommon(raftGroupId, RAFT_INIT_FLAG_KEY);
  }


  /**
   * commit key = 1个字节（类型）+4个字节（raft group id） ，已经提交的记录
   * @param raftGroupId
   * @return
   */
  public static byte[] generateCommitLogKey(int raftGroupId) {
    return generateCommon(raftGroupId, COMMIT_LOG_KEY_PREFIX);
  }

  private static byte[] generateCommon(int raftGroupId, byte type) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(5);
    byteBuffer.put(type);
    byteBuffer.putInt(raftGroupId);
    return byteBuffer.array();
  }

  private static byte[] generateCommon(long transactionId, byte type) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(9);
    byteBuffer.put(type);
    byteBuffer.putLong(transactionId);
    return byteBuffer.array();
  }

  /**
   * apply key = 1个字节（类型）+4个字节（raft group id） ， 已经被应用的key
   * @param raftGroupId
   * @return
   */
  public static byte[] generateApplyLogKey(int raftGroupId) {
    return generateCommon(raftGroupId, APPLY_LOG_KEY_PREFIX);
  }

  /**
   * long key = 1个字节（类型）+4个字节（raft group id）+8个字节（logindex） ，log日志
   * @param raftGroupId
   * @param logindex
   * @return
   */
  public static byte[] generateLogKey(int raftGroupId,long logindex){
    ByteBuffer byteBuffer = ByteBuffer.allocate(13);
    byteBuffer.put(LOG_KEY_PREFIX);
    byteBuffer.putInt(raftGroupId);
    byteBuffer.putLong(logindex);
    return byteBuffer.array();
  }

  /**
   * long key = 1个字节（类型）+4个字节（raft group id） ，数据前缀
   * @param raftGroupId
   * @return
   */
  public static byte[] generateDataKey(int raftGroupId){
    return generateCommon(raftGroupId, DATA_KEY_PREFIX);
  }

  // ==================== MVCC 相关 Key 生成方法 ====================

  /**
   * 生成 MVCC 数据 Key（扁平化存储）
   * Key 格式：1 字节类型 + 4 字节 groupId + N 字节 userKey + 8 字节 transactionId
   * @param groupId Raft 组 ID
   * @param userKey 用户数据 Key
   * @param transactionId 事务 ID
   * @return MVCC 数据 Key
   */
  public static byte[] generateMVCCDataKey(int groupId, byte[] userKey, long transactionId) {
    ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + userKey.length + 8);
    buffer.put(MVCC_DATA_KEY_PREFIX);
    buffer.putInt(groupId);
    buffer.put(userKey);
    buffer.putLong(transactionId);
    return buffer.array();
  }

  /**
   * 生成 MVCC 数据 Key 前缀（用于范围查询）
   * Key 格式：1 字节类型 + 4 字节 groupId + N 字节 userKey
   * @param groupId Raft 组 ID
   * @param userKey 用户数据 Key
   * @return MVCC 数据 Key 前缀
   */
  public static byte[] generateMVCCDataKeyPrefix(int groupId, byte[] userKey) {
    ByteBuffer buffer = ByteBuffer.allocate(1 + 4 + userKey.length);
    buffer.put(MVCC_DATA_KEY_PREFIX);
    buffer.putInt(groupId);
    buffer.put(userKey);
    return buffer.array();
  }

  /**
   * 生成 MVCC 数据 Key 范围起始 Key（用于查询某个 userKey 的所有版本）
   * @param groupId Raft 组 ID
   * @param userKey 用户数据 Key
   * @return 起始 Key
   */
  public static byte[] generateMVCCDataKeyStart(int groupId, byte[] userKey) {
    return generateMVCCDataKeyPrefix(groupId, userKey);
  }

  /**
   * 生成 MVCC 数据 Key 范围结束 Key（用于查询某个 userKey 的所有版本）
   * @param groupId Raft 组 ID
   * @param userKey 用户数据 Key
   * @return 结束 Key
   */
  public static byte[] generateMVCCDataKeyEnd(int groupId, byte[] userKey) {
    byte[] prefix = generateMVCCDataKeyPrefix(groupId, userKey);
    // 将前缀的最后一个字节 +1，得到范围查询的结束 Key
    byte[] endKey = new byte[prefix.length + 8];
    System.arraycopy(prefix, 0, endKey, 0, prefix.length);
    // 填充 8 个字节的 0xFF，确保覆盖所有 transactionId
    for (int i = prefix.length; i < endKey.length; i++) {
      endKey[i] = (byte) 0xFF;
    }
    return endKey;
  }

  /**
   * 生成 MVCC 全局最后提交时间戳 Key
   * @return MVCC 全局最后提交时间戳 Key
   */
  public static byte[] generateMVCCLastCommitTsKey() {
    ByteBuffer buffer = ByteBuffer.allocate(9);
    buffer.put(MVCC_LAST_COMMIT_TS_PREFIX);
    buffer.putLong(0L);
    return buffer.array();
  }
}
