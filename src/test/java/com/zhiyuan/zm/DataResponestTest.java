package com.zhiyuan.zm;

import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.DataResponest;
import org.junit.Assert;
import org.junit.Test;

/**
 * DataResponest 响应类测试
 * @author zhouzhiyuan
 * @date 2026/03/03
 */
public class DataResponestTest {

    @Test
    public void testConstructor() {
        DataResponest response = new DataResponest();
        Assert.assertEquals(0, response.getStatus());
        Assert.assertNull(response.getMessage());
    }

    @Test
    public void testConstructorWithStatus() {
        DataResponest response = new DataResponest(StatusCode.SUCCESS);
        Assert.assertEquals(StatusCode.SUCCESS, response.getStatus());
        Assert.assertNull(response.getMessage());
    }

    @Test
    public void testConstructorWithMessage() {
        String message = "test message";
        DataResponest response = new DataResponest(message);
        Assert.assertEquals(StatusCode.SUCCESS, response.getStatus());
        Assert.assertEquals(message, response.getMessage());
    }

    @Test
    public void testConstructorWithStatusAndMessage() {
        DataResponest response = new DataResponest(StatusCode.REDIRECT, "redirect message");
        Assert.assertEquals(StatusCode.REDIRECT, response.getStatus());
        Assert.assertEquals("redirect message", response.getMessage());
    }

    @Test
    public void testIsSuccess() {
        DataResponest successResponse = new DataResponest(StatusCode.SUCCESS, "success");
        Assert.assertTrue(successResponse.isSuccess());

        DataResponest failResponse = new DataResponest(StatusCode.REDIRECT, "redirect");
        Assert.assertFalse(failResponse.isSuccess());
    }

    @Test
    public void testSetters() {
        DataResponest response = new DataResponest();
        response.setStatus(StatusCode.SUCCESS);
        response.setMessage("new message");

        Assert.assertEquals(StatusCode.SUCCESS, response.getStatus());
        Assert.assertEquals("new message", response.getMessage());
    }

    @Test
    public void testToString() {
        DataResponest response = new DataResponest(StatusCode.SUCCESS, "test");
        String str = response.toString();
        Assert.assertNotNull(str);
        Assert.assertTrue(str.contains("status=200"));
        Assert.assertTrue(str.contains("message='test'"));
    }
}
