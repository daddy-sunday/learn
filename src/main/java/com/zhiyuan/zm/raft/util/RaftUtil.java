package com.zhiyuan.zm.raft.util;

import java.nio.ByteBuffer;

/**
 *直接看下面的方法注释，更容易理解前缀key
 * @author zhouzhiyuan
 *@date 2022/5/10
 */
public class RaftUtil {


  public static final long  INIT_LOG_INDEX = 5;
  public static final long  INIT_TERM = 5;

  private static final byte COMMIT_LOG_KEY_PREFIX = 1;

  private static final byte APPLY_LOG_KEY_PREFIX = 2;

  private static final byte LOG_KEY_PREFIX = 10;

  private static final byte RAFT_INIT_FLAG_KEY = 3;

  private static final byte DATA_KEY_PREFIX = 20;


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
}
