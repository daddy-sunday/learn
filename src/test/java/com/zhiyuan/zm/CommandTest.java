package com.zhiyuan.zm;

import com.zhiyuan.zm.raft.constant.DataOperationType;
import com.zhiyuan.zm.raft.dto.Command;
import com.zhiyuan.zm.raft.dto.Row;
import org.junit.Assert;
import org.junit.Test;

/**
 * Command DTO 测试
 * @author zhouzhiyuan
 * @date 2026/03/03
 */
public class CommandTest {

    @Test
    public void testConstructorWithCmd() {
        Command command = new Command(DataOperationType.INSERT);
        Assert.assertEquals(DataOperationType.INSERT, command.getCmd());
        Assert.assertNull(command.getRows());
    }

    @Test
    public void testConstructorWithCmdAndRows() {
        Row[] rows = new Row[]{new Row("key".getBytes(), "value".getBytes())};
        Command command = new Command(DataOperationType.INSERT, rows);
        Assert.assertEquals(DataOperationType.INSERT, command.getCmd());
        Assert.assertArrayEquals(rows, command.getRows());
    }

    @Test
    public void testSetters() {
        Command command = new Command();
        command.setCmd(DataOperationType.DELETE);
        Row[] rows = new Row[]{new Row("key".getBytes(), (byte[]) null)};
        command.setRows(rows);

        Assert.assertEquals(DataOperationType.DELETE, command.getCmd());
        Assert.assertArrayEquals(rows, command.getRows());
    }

    @Test
    public void testOperationTypeEnum() {
        Assert.assertEquals((byte) 3, DataOperationType.INSERT);
        Assert.assertEquals((byte) 1, DataOperationType.DELETE);
        Assert.assertEquals((byte) 2, DataOperationType.UPDATE);
        Assert.assertEquals((byte) 4, DataOperationType.EMPTY);
    }
}
