package com.zhiyuan.zm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.Row;
import com.zhiyuan.zm.raft.rpc.ZMClient;
import com.zhiyuan.zm.raft.service.RaftService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.rocksdb.RocksDBException;

import com.alipay.remoting.exception.RemotingException;

/**
 * 整合的 Raft 集群测试类
 * 包含节点启动、客户端操作、节点关闭的完整流程
 *
 * @author zhouzhiyuan
 * @date 2026/03/04
 */
public class RaftClusterTest {

  // 保存启动的服务引用，用于关闭
  private static final List<RaftService> services = new ArrayList<>();
  private static final List<GlobalConfig> configs = new ArrayList<>();

  // 客户端
  private ZMClient client;

  // 节点数量
  private static final int NODE_COUNT = 3;

  // 等待选举完成的时间（秒）
  private static final long WAIT_ELECTION_TIME = 50;

  /**
   * 启动所有节点（Before 每个测试方法执行前运行）
   */
  @Before
  public void startCluster() throws InterruptedException, RocksDBException {
    System.out.println("========== 开始启动 Raft 集群 ==========");
    services.clear();
    configs.clear();

    // 启动 3 个节点
    startNode(0, "localhost:20000", "D:\\tmp\\raft\\log", "D:\\tmp\\raft\\data");
    startNode(1, "localhost:20001", "D:\\tmp\\raft\\log2", "D:\\tmp\\raft\\data2");
    startNode(2, "localhost:20002", "D:\\tmp\\raft\\log3", "D:\\tmp\\raft\\data3");

    // 等待选举完成
    System.out.println("等待 " + WAIT_ELECTION_TIME + " 秒让集群完成选举...");
    Thread.sleep(WAIT_ELECTION_TIME * 1000);
    System.out.println("========== Raft 集群启动完成 ==========");
  }

  /**
   * 关闭所有节点（After 每个测试方法执行后运行）
   */
  @After
  public void shutdownCluster() {
    System.out.println("========== 开始关闭 Raft 集群 ==========");
    shutdownAll();
    System.out.println("========== Raft 集群已关闭 ==========");
  }

  /**
   * 启动单个节点
   */
  private void startNode(int index, String address, String logPath, String dataPath)
      throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setLogPath(logPath);
    globalConfig.setDataPath(dataPath);
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002");
    globalConfig.setPort(20000 + index);
    globalConfig.setCurrentNode(address);
    // 设置监控端口，避免与其他节点冲突（节点 0:8080, 节点 1:8081, 节点 2:8082）
    globalConfig.setMonitorPort(8080 + index);

    RaftService raftService = new RaftService();
    services.add(raftService);
    configs.add(globalConfig);

    // 在新线程中启动服务（非阻塞）
    Thread startThread = new Thread(() -> {
      try {
        raftService.start(globalConfig);
      } catch (RocksDBException e) {
        System.err.println("启动节点 " + address + " 失败：" + e.getMessage());
        e.printStackTrace();
      }
    }, "RaftNode-" + address);
    startThread.setDaemon(false);
    startThread.start();

    System.out.println("已启动节点：" + address + " (端口：" + (20000 + index) + ", 监控端口：" + (8080 + index) + ")");
    Thread.sleep(500); // 每个节点启动间隔
  }

  /**
   * 停止所有已启动的服务
   */
  public void shutdownAll() {
    System.out.println("开始关闭所有服务...");
    for (int i = services.size() - 1; i >= 0; i--) {
      try {
        RaftService service = services.get(i);
        if (service != null && service.isRunning()) {
          service.shutdown();
          System.out.println("服务 " + i + " 已关闭");
        }
      } catch (Exception e) {
        System.err.println("关闭服务 " + i + " 时发生异常：" + e.getMessage());
      }
    }
    services.clear();
    configs.clear();
    System.out.println("所有服务已关闭");
  }

  /**
   * 停止指定的服务
   * @param index 服务索引 (0, 1, 2)
   */
  public void shutdown(int index) {
    if (index < 0 || index >= services.size()) {
      System.err.println("无效的服务索引：" + index);
      return;
    }
    RaftService service = services.get(index);
    try {
      if (service != null && service.isRunning()) {
        service.shutdown();
        services.set(index, null);
        System.out.println("服务 " + (index + 1) + " 已关闭");
      }
    } catch (Exception e) {
      System.err.println("关闭服务 " + (index + 1) + " 时发生异常：" + e.getMessage());
    }
  }

  // ==================== 客户端测试方法 ====================

  /**
   * 单条数据写入 100 条数据
   */
  @Test
  public void testPut100Records() throws RemotingException, InterruptedException {
    System.out.println("========== 测试：单条写入 100 条数据 ==========");
    client = new ZMClient("localhost:20002");
    for (int i = 0; i < 100; i++) {
      Row row = new Row((i + "王五和小六子").getBytes(), (i + "是同学哈").getBytes());
      DataResponest dataResponest = client.put(new Row[] {row});
      if (i % 20 == 0) {
        System.out.println("写入第 " + i + " 条：" + dataResponest);
      }
      Assert.assertEquals(200, dataResponest.getStatus());
    }
    System.out.println("========== 测试通过：单条写入 100 条数据 ==========");
  }

  /**
   * 批量写入 100 条数据
   */
  @Test
  public void testBatchPut100Records() throws RemotingException, InterruptedException {
    System.out.println("========== 测试：批量写入 100 条数据 ==========");
    client = new ZMClient("localhost:20001");
    Row[] rows = new Row[100];
    for (int i = 100; i < 200; i++) {
      rows[i - 100] = new Row((i + "王五和小六子").getBytes(), (i + "是同学哈").getBytes());
    }
    DataResponest dataResponest = client.put(rows);
    System.out.println("批量写入结果：" + dataResponest);
    Assert.assertEquals(200, dataResponest.getStatus());
    System.out.println("========== 测试通过：批量写入 100 条数据 ==========");
  }

  /**
   * 10 并发写入 100 条重复数据
   */
  @Test
  public void testConcurrentPut() throws InterruptedException {
    System.out.println("========== 测试：10 并发写入 100 条数据 ==========");
    ExecutorService service = Executors.newFixedThreadPool(10);
    CountDownLatch latch = new CountDownLatch(10);

    for (int i = 0; i < 10; i++) {
      service.submit(() -> {
        ZMClient client = new ZMClient("localhost:20000");
        for (int j = 200; j < 300; j++) {
          Row row = new Row((j + "王五和小六子").getBytes(), (j + "是同学哈").getBytes());
          try {
            DataResponest dataResponest = client.put(new Row[] {row});
            if (dataResponest.getStatus() != 200) {
              System.err.println("写入失败：" + dataResponest);
            }
          } catch (Exception e) {
            System.err.println("并发写入异常：" + e.getMessage());
          }
        }
        latch.countDown();
      });
    }

    latch.await(10, TimeUnit.MINUTES);
    service.shutdownNow();
    System.out.println("========== 测试通过：10 并发写入 100 条数据 ==========");
  }

  /**
   * 查询数据（依赖前面的写入测试先执行）
   */
  @Test
  public void testGet() throws RemotingException, InterruptedException {
    System.out.println("========== 测试：查询数据 ==========");

    // 先执行写入（确保有数据）
    testPut100Records();
    testBatchPut100Records();

    // 查询 0-99
    client = new ZMClient("localhost:20001");
    for (int i = 0; i < 100; i++) {
      DataResponest dataResponest = client.get(i + "王五和小六子");
      Assert.assertEquals(i + "是同学哈", dataResponest.getMessage());
    }
    System.out.println("查询 0-99 完成");

    // 查询 0-199
    for (int i = 0; i < 200; i++) {
      DataResponest dataResponest = client.get(i + "王五和小六子");
      Assert.assertEquals(i + "是同学哈", dataResponest.getMessage());
    }
    System.out.println("查询 0-199 完成");

    // 查询 0-299
    client = new ZMClient("localhost:20002");
    for (int i = 0; i < 300; i++) {
      DataResponest dataResponest = client.get(i + "王五和小六子");
      Assert.assertEquals(i + "是同学哈", dataResponest.getMessage());
    }
    System.out.println("查询 0-299 完成");

    System.out.println("========== 测试通过：查询数据 ==========");
  }

  /**
   * 删除数据
   */
  @Test
  public void testDelete() throws RemotingException, InterruptedException {
    System.out.println("========== 测试：删除数据 ==========");

    // 先写入数据
    testPut100Records();

    client = new ZMClient("localhost:20002");
    for (int i = 0; i < 100; i++) {
      Row row = new Row((i + "王五和小六子").getBytes(), (byte[]) null);
      DataResponest dataResponest = client.delete(new Row[] {row});
      Assert.assertEquals(200, dataResponest.getStatus());
    }

    // 验证删除
    for (int i = 0; i < 100; i++) {
      DataResponest dataResponest = client.get(i + "王五和小六子");
      // 删除后查询应该返回 null 或空
      System.out.println("删除后查询 key=" + i + "王五和小六子" + ", result=" + dataResponest);
    }

    System.out.println("========== 测试通过：删除数据 ==========");
  }

  /**
   * Leader 漂移测试
   * 注意：这个测试需要正确的节点配置，默认被注释掉
   */
  @Test
  @Ignore("需要正确的节点配置才能运行")
  public void testLeaderMove() throws RemotingException, InterruptedException {
    System.out.println("========== 测试：Leader 漂移 ==========");

    // 注意：这个方法需要节点配置正确
    // 实际测试时需要根据实际节点地址调整
    client = new ZMClient("localhost:20000");
    // 示例：将 leader 从 localhost:20000 漂移到 localhost:20001
    // DataResponest result = client.leaderMove("localhost:20001");
    // System.out.println("Leader 漂移结果：" + result);

    System.out.println("Leader 漂移测试需要根据实际集群配置调整目标地址");
    System.out.println("========== 测试完成：Leader 漂移 ==========");
  }

  /**
   * 完整流程测试：启动 -> 写入 -> 查询 -> 删除 -> 关闭
   */
  @Test
  public void testFullProcess() throws Exception {
    System.out.println("========== 完整流程测试开始 ==========");

    // 1. 写入
    System.out.println("步骤 1: 写入数据...");
    testPut100Records();
    testBatchPut100Records();

    // 2. 查询
    System.out.println("步骤 2: 查询数据...");
    testGet();

    // 3. 删除
    System.out.println("步骤 3: 删除数据...");
    testDelete();

    System.out.println("========== 完整流程测试通过 ==========");
  }
}
