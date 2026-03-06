package com.zhiyuan.zm.raft.monitor;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.service.RaftService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

/**
 * 监控服务集成测试
 * 测试监控服务是否正常启动和响应
 *
 * @author zhouzhiyuan
 * @date 2026/03/06
 */
public class MonitorServiceIntegrationTest {

    private final List<RaftService> services = new ArrayList<>();

    @After
    public void shutdownAll() {
        for (RaftService service : services) {
            if (service != null && service.isRunning()) {
                try {
                    service.shutdown();
                } catch (Exception e) {
                    System.err.println("关闭服务时发生异常：" + e.getMessage());
                }
            }
        }
        services.clear();
    }

    /**
     * 测试监控服务是否正常启动
     */
    @Test
    public void testMonitorServiceStarts() throws Exception {
        GlobalConfig config = new GlobalConfig();
        config.setOtherNode("localhost:20000,localhost:20001,localhost:20002");
        config.setPort(20000);
        config.setCurrentNode("localhost:20000");
        config.setMonitorPort(8090); // 使用特殊端口避免冲突
        config.setMonitorEnabled(true);

        RaftService service = new RaftService();
        services.add(service);

        // 启动服务（在新线程中，因为 start 是阻塞的）
        Thread startThread = new Thread(() -> {
            try {
                service.start(config);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "TestRaftService");
        startThread.setDaemon(true);
        startThread.start();

        // 等待服务启动
        Thread.sleep(3000);

        // 测试健康检查接口
        URL url = new URL("http://localhost:8090/api/health");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);

        int responseCode = conn.getResponseCode();
        Assert.assertEquals("监控服务健康检查失败", 200, responseCode);

        System.out.println("监控服务启动成功，健康检查通过！");
    }

    /**
     * 测试集群状态 API
     */
    @Test
    public void testClusterStatusApi() throws Exception {
        GlobalConfig config = new GlobalConfig();
        config.setOtherNode("localhost:20000,localhost:20001,localhost:20002");
        config.setPort(20000);
        config.setCurrentNode("localhost:20000");
        config.setMonitorPort(8091);
        config.setMonitorEnabled(true);

        RaftService service = new RaftService();
        services.add(service);

        Thread startThread = new Thread(() -> {
            try {
                service.start(config);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "TestRaftService");
        startThread.setDaemon(true);
        startThread.start();

        Thread.sleep(3000);

        // 测试集群状态接口
        URL url = new URL("http://localhost:8091/api/cluster");
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(5000);

        int responseCode = conn.getResponseCode();
        Assert.assertEquals("集群状态 API 失败", 200, responseCode);

        System.out.println("集群状态 API 测试通过！");
    }

    /**
     * 测试禁用监控服务
     */
    @Test
    public void testMonitorServiceDisabled() throws Exception {
        GlobalConfig config = new GlobalConfig();
        config.setOtherNode("localhost:20000,localhost:20001,localhost:20002");
        config.setPort(20000);
        config.setCurrentNode("localhost:20000");
        config.setMonitorPort(8092);
        config.setMonitorEnabled(false); // 禁用监控

        RaftService service = new RaftService();
        services.add(service);

        Thread startThread = new Thread(() -> {
            try {
                service.start(config);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }, "TestRaftService");
        startThread.setDaemon(true);
        startThread.start();

        Thread.sleep(2000);

        // 监控服务应该无法访问
        try {
            URL url = new URL("http://localhost:8092/api/health");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(2000);
            conn.connect();
            // 如果能连接上，说明测试失败
            Assert.fail("监控服务应该被禁用，但仍然可以访问");
        } catch (Exception e) {
            // 预期异常，测试通过
            System.out.println("监控服务禁用测试通过！");
        }
    }
}
