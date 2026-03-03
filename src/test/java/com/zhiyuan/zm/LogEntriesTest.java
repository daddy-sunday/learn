package com.zhiyuan.zm;

import com.zhiyuan.zm.raft.dto.LogEntries;
import org.junit.Assert;
import org.junit.Test;

/**
 * LogEntries DTO 测试
 * @author zhouzhiyuan
 * @date 2026/03/03
 */
public class LogEntriesTest {

    @Test
    public void testConstructor() {
        LogEntries entry = new LogEntries(100L, 1L, "test command");
        Assert.assertEquals(100L, entry.getLogIndex());
        Assert.assertEquals(1L, entry.getTerm());
        Assert.assertEquals("test command", entry.getMesssage());
    }

    @Test
    public void testSetters() {
        LogEntries entry = new LogEntries();
        entry.setLogIndex(200L);
        entry.setTerm(2L);
        entry.setMesssage("new command");

        Assert.assertEquals(200L, entry.getLogIndex());
        Assert.assertEquals(2L, entry.getTerm());
        Assert.assertEquals("new command", entry.getMesssage());
    }

    @Test
    public void testToString() {
        LogEntries entry = new LogEntries(100L, 1L, "test");
        String str = entry.toString();
        Assert.assertNotNull(str);
        Assert.assertTrue(str.contains("logIndex=100"));
        Assert.assertTrue(str.contains("term=1"));
        Assert.assertTrue(str.contains("messsage='test'"));
    }
}
