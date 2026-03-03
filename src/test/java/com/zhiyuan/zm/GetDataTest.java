package com.zhiyuan.zm;

import com.zhiyuan.zm.raft.dto.GetData;
import org.junit.Assert;
import org.junit.Test;

/**
 * GetData DTO 测试
 * @author zhouzhiyuan
 * @date 2026/03/03
 */
public class GetDataTest {

    @Test
    public void testConstructor() {
        String key = "test_key";
        GetData getData = new GetData(key);
        Assert.assertEquals(key, getData.getKey());
    }

    @Test
    public void testSetters() {
        GetData getData = new GetData("initial_key");
        getData.setKey("new_key");
        Assert.assertEquals("new_key", getData.getKey());
    }
}
