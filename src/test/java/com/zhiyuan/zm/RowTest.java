package com.zhiyuan.zm;

import com.zhiyuan.zm.raft.dto.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Row DTO 测试
 * @author zhouzhiyuan
 * @date 2026/03/03
 */
public class RowTest {

    @Test
    public void testConstructorWithBytes() {
        byte[] key = "key".getBytes();
        byte[] value = "value".getBytes();
        Row row = new Row(key, value);
        Assert.assertArrayEquals(key, row.getKey());
        Assert.assertArrayEquals(value, row.getValue());
    }

    @Test
    public void testConstructorWithLongs() {
        Long key = 100L;
        Long value = 200L;
        Row row = new Row(key, value);
        Assert.assertNotNull(row.getKey());
        Assert.assertNotNull(row.getValue());
    }

    @Test
    public void testConstructorWithObject() {
        byte[] key = "key".getBytes();
        String obj = "test object";
        Row row = new Row(key, obj);
        Assert.assertArrayEquals(key, row.getKey());
        Assert.assertNotNull(row.getValue());
    }

    @Test
    public void testSetters() {
        Row row = new Row("key".getBytes(), "value".getBytes());
        byte[] newKey = "newKey".getBytes();
        byte[] newValue = "newValue".getBytes();
        row.setKey(newKey);
        row.setValue(newValue);
        Assert.assertArrayEquals(newKey, row.getKey());
        Assert.assertArrayEquals(newValue, row.getValue());
    }
}
