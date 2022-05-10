package org.example.raft.util;

import java.nio.ByteBuffer;

/**
 *@author zhouzhiyuan
 *@date 2022/5/10
 */
public class RaftUtil {

  /**
   *
   */
  private static final byte COMMIT_LOG_KEY_PREFIX = 1;

  private static final byte APPLY_LOG_KEY_PREFIX = 2;

  private static final byte LOG_KEY_PREFIX = 3;

  /**
   * commit key = 1个字节（类型）+4个字节（raft group id）
   * @param raftGroupId
   * @return
   */
  public static byte[] generateCommitLogKey(int raftGroupId){
    ByteBuffer byteBuffer = ByteBuffer.allocate(5);
    byteBuffer.put(COMMIT_LOG_KEY_PREFIX);
    byteBuffer.putInt(raftGroupId);
    return byteBuffer.array();
  }

  /**
   * apply key = 1个字节（类型）+4个字节（raft group id）
   * @param raftGroupId
   * @return
   */
  public static byte[] generateApplyLogKey(int raftGroupId){
    ByteBuffer byteBuffer = ByteBuffer.allocate(5);
    byteBuffer.put(APPLY_LOG_KEY_PREFIX);
    byteBuffer.putInt(raftGroupId);
    return byteBuffer.array();
  }

  /**
   * long key = 1个字节（类型）+4个字节（raft group id）+8个字节（logindex）
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

}
