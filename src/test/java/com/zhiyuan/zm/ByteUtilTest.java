package com.zhiyuan.zm;

import com.zhiyuan.zm.raft.util.ByteUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * ByteUtil 工具类测试
 * @author zhouzhiyuan
 * @date 2026/03/03
 */
public class ByteUtilTest {

    @Test
    public void testLongToBytes() {
        long value = 123456789L;
        byte[] bytes = ByteUtil.longToBytes(value);
        Assert.assertNotNull(bytes);
        Assert.assertEquals(8, bytes.length);
    }

    @Test
    public void testBytesToLong() {
        long value = 987654321L;
        byte[] bytes = ByteUtil.longToBytes(value);
        long result = ByteUtil.bytesToLong(bytes);
        Assert.assertEquals(value, result);
    }

    @Test
    public void testLongToBytesAndBack() {
        long[] testValues = {0L, 1L, -1L, Long.MAX_VALUE, Long.MIN_VALUE, 123456L};
        for (long value : testValues) {
            byte[] bytes = ByteUtil.longToBytes(value);
            long result = ByteUtil.bytesToLong(bytes);
            Assert.assertEquals("转换失败：" + value, value, result);
        }
    }

    @Test
    public void testConcatBytes() {
        byte[] prefix = new byte[]{1, 2, 3};
        byte[] suffix = new byte[]{4, 5, 6};
        byte[] result = ByteUtil.concatBytes(prefix, suffix);
        Assert.assertNotNull(result);
        Assert.assertEquals(6, result.length);
        Assert.assertArrayEquals(new byte[]{1, 2, 3, 4, 5, 6}, result);
    }

    @Test
    public void testConcatBytesWithEmpty() {
        byte[] prefix = new byte[]{1, 2, 3};
        byte[] empty = new byte[]{};
        byte[] result = ByteUtil.concatBytes(prefix, empty);
        Assert.assertEquals(3, result.length);
        Assert.assertArrayEquals(prefix, result);
    }
}
