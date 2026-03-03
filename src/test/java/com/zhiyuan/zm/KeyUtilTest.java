package com.zhiyuan.zm;

import com.zhiyuan.zm.raft.util.KeyUtil;
import org.junit.Assert;
import org.junit.Test;

/**
 * KeyUtil 工具类测试
 * @author zhouzhiyuan
 * @date 2026/03/03
 */
public class KeyUtilTest {

    @Test
    public void testGenerateRaftInitKey() {
        int groupId = 1;
        byte[] key = KeyUtil.generateRaftInitKey(groupId);
        Assert.assertNotNull(key);
        Assert.assertTrue(key.length > 0);
    }

    @Test
    public void testGenerateLogKey() {
        int groupId = 1;
        long logIndex = 100L;
        byte[] key = KeyUtil.generateLogKey(groupId, logIndex);
        Assert.assertNotNull(key);
        Assert.assertTrue(key.length > 0);
    }

    @Test
    public void testGenerateApplyLogKey() {
        int groupId = 1;
        byte[] key = KeyUtil.generateApplyLogKey(groupId);
        Assert.assertNotNull(key);
        Assert.assertTrue(key.length > 0);
    }

    @Test
    public void testGenerateDataKey() {
        int groupId = 1;
        byte[] key = KeyUtil.generateDataKey(groupId);
        Assert.assertNotNull(key);
        Assert.assertTrue(key.length > 0);
    }

    @Test
    public void testGenerateCacheTransactionIdKey() {
        byte[] key = KeyUtil.generateCacheTransactionIdKey();
        Assert.assertNotNull(key);
        Assert.assertTrue(key.length > 0);
    }

    @Test
    public void testGenerateTransactionIdKey() {
        long transactionId = 123456L;
        byte[] key = KeyUtil.generateTransactionIdKey(transactionId);
        Assert.assertNotNull(key);
        Assert.assertTrue(key.length > 0);
    }

    @Test
    public void testKeyUniqueness() {
        byte[] key1 = KeyUtil.generateLogKey(1, 100L);
        byte[] key2 = KeyUtil.generateLogKey(1, 101L);
        byte[] key3 = KeyUtil.generateLogKey(2, 100L);

        Assert.assertFalse("不同 logIndex 生成的 key 应该不同", bytesEquals(key1, key2));
        Assert.assertFalse("不同 groupId 生成的 key 应该不同", bytesEquals(key1, key3));
    }

    private boolean bytesEquals(byte[] a, byte[] b) {
        if (a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }
}
