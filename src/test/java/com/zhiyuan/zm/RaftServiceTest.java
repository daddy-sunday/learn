package com.zhiyuan.zm;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.service.RaftService;
import org.junit.After;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import java.util.ArrayList;
import java.util.List;

/**
 * Raft 节点启动测试类
 *
 * 使用说明：
 * 1. 独立启动节点测试：运行 server1/server2/server3 方法
 * 2. 完整集群测试：请使用 {@link RaftClusterTest} 类
 * 3. 客户端测试：请使用 {@link ZMClientTest} 类
 *
 * @author zhouzhiyuan
 * @date 2021/12/10
 */
public class RaftServiceTest {

  // 保存启动的服务引用，用于关闭
  private final List<RaftService> services = new ArrayList<>();

  /**
   * 关闭所有已启动的服务（在每个测试方法执行后自动调用）
   */
  @After
  public void shutdownAll() {
    System.out.println("开始关闭所有服务...");
    for (int i = services.size() - 1; i >= 0; i--) {
      try {
        RaftService service = services.get(i);
        if (service != null && service.isRunning()) {
          service.shutdown();
          System.out.println("服务 " + (i + 1) + " 已关闭");
        }
      } catch (Exception e) {
        System.err.println("关闭服务时发生异常：" + e.getMessage());
      }
    }
    services.clear();
    System.out.println("所有服务已关闭");
  }

  /**
   * 启动一个存储节点（节点 1 - 端口 20000）
   * 注意：这个方法是阻塞的，会一直运行直到手动停止
   */
  @Test
  public void server1() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002");
    globalConfig.setPort(20000);
    globalConfig.setCurrentNode("localhost:20000");
    RaftService raftService = new RaftService();
    services.add(raftService);
    raftService.start(globalConfig);
  }

  /**
   * 启动一个存储节点（节点 2 - 端口 20001）
   * 注意：这个方法是阻塞的，会一直运行直到手动停止
   */
  @Test
  public void server2() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setLogPath("D:\\tmp\\raft\\log2");
    globalConfig.setDataPath("D:\\tmp\\raft\\data2");
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002");
    globalConfig.setPort(20001);
    globalConfig.setCurrentNode("localhost:20001");
    RaftService raftService = new RaftService();
    services.add(raftService);
    raftService.start(globalConfig);
  }

  /**
   * 启动一个存储节点（节点 3 - 端口 20002）
   * 注意：这个方法是阻塞的，会一直运行直到手动停止
   */
  @Test
  public void server3() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setLogPath("D:\\tmp\\raft\\log3");
    globalConfig.setDataPath("D:\\tmp\\raft\\data3");
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002");
    globalConfig.setPort(20002);
    globalConfig.setCurrentNode("localhost:20002");
    RaftService raftService = new RaftService();
    services.add(raftService);
    raftService.start(globalConfig);
  }

  /**
   * 手动关闭指定的服务
   * @param index 服务索引 (1, 2, 3)
   */
  public void shutdown(int index) {
    if (index < 1 || index > services.size()) {
      System.err.println("无效的服务索引：" + index);
      return;
    }
    RaftService service = services.get(index - 1);
    try {
      if (service != null && service.isRunning()) {
        service.shutdown();
        services.set(index - 1, null);
        System.out.println("服务 " + index + " 已关闭");
      }
    } catch (Exception e) {
      System.err.println("关闭服务 " + index + " 时发生异常：" + e.getMessage());
    }
  }
}
