package org.example.raft.util;

import java.nio.ByteBuffer;

/**
 *@author zhouzhiyuan
 *@date 2021/10/25
 */
public class ByteUtil {

  /**
   * todo 不想 把Bits里面的方法重写一边，先凑合用
   * @param l
   * @return
   */
  public static byte[] longToBytes(long l) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.putLong(0, l);
    return byteBuffer.array();
  }

  public static byte[] intToBytes(int l) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    byteBuffer.putInt(l);
    return byteBuffer.array();
  }

  public static byte[] concatLogId(long l1, long l2) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(17);
    byteBuffer.putLong(l1);
    byteBuffer.put("_".getBytes());
    byteBuffer.putLong(l2);
    return byteBuffer.array();
  }

  public static String parse17(byte[] key) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(17);
    byteBuffer.put(key);
    byteBuffer.flip();
    long groupid = byteBuffer.getLong();
    byte b = byteBuffer.get();
    String s = new String(new byte[] {b});
    long longId = byteBuffer.getLong();
    return groupid + s + longId;
  }

  public static int bytesCompare(byte[] param1, byte[] param2) {
    int len1 = param1.length;
    int len2 = param2.length;
    int lim = Math.min(len1, len2);
    byte v1[] = param1;
    byte v2[] = param2;
    int k = 0;
    while (k < lim) {
      byte c1 = v1[k];
      byte c2 = v2[k];
      if (c1 != c2) {
        return c1 - c2;
      }
      k++;
    }
    return len1 - len2;
  }

  public static byte[] concatBytes(byte[] p1, byte[] p2){
    int l1 = p1.length;
    int l2 = p2.length;
    byte[] result = new byte[l1 + l2];
    System.arraycopy(p1,0,result,0, l1);
    System.arraycopy(p2,0,result, l1, l2);
    return result;
  }
}
