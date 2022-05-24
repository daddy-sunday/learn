package org.example;

import java.nio.ByteBuffer;

/**
 *@author zhouzhiyuan
 *@date 2021/10/25
 */
public class TestByteUtil {

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

  public static Long bytesToLong(byte[] bytes) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(8);
    byteBuffer.put(bytes);
    byteBuffer.flip();
    return byteBuffer.getLong();
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

  public static String parseDataKey(byte[] key) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(key.length);
    byteBuffer.put(key);
    byteBuffer.flip();
    if (key.length == 5) {
      byte b = byteBuffer.get();
      int groupid = byteBuffer.getInt();
      return b + "_" + groupid;
    } else {
      byte b = byteBuffer.get();
      int groupid = byteBuffer.getInt();
      byte[] bytes = new byte[key.length-5];
      System.arraycopy(key,5,bytes,0,bytes.length);
      String s = new String(bytes);
      return b +"_" + groupid+"_"+s;
    }
  }

  public static String parseLogKey(byte[] key) {
    ByteBuffer byteBuffer = ByteBuffer.allocate(key.length);
    byteBuffer.put(key);
    byteBuffer.flip();
    if (key.length == 5) {
      byte b = byteBuffer.get();
      int groupid = byteBuffer.getInt();
      return b + "_" + groupid;
    } else  if (key.length == 13) {
      byte b = byteBuffer.get();
      int groupid = byteBuffer.getInt();
      long logId = byteBuffer.getLong();
      return b + "_" + groupid + "_" + logId;
    }else {
      throw new RuntimeException("有未知的key");
    }
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

  public static byte[] concatBytes(byte[] p1, byte[] p2) {
    int l1 = p1.length;
    int l2 = p2.length;
    byte[] result = new byte[l1 + l2];
    System.arraycopy(p1, 0, result, 0, l1);
    System.arraycopy(p2, 0, result, l1, l2);
    return result;
  }
}
