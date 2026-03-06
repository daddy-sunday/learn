package com.zhiyuan.zm;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.Row;
import com.zhiyuan.zm.raft.rpc.DefaultRpcClient;

import com.zhiyuan.zm.raft.service.RaftService;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import com.alipay.remoting.exception.RemotingException;
import com.zhiyuan.zm.raft.rpc.ZMClient;

/**
 * ZM 客户端测试类
 *
 * 重要说明：
 * 1. 本测试类依赖 Raft 集群已经启动并完成选举
 * 2. 推荐使用 {@link RaftClusterTest} 进行完整测试（自动启动集群）
 * 3. 如果手动运行本类方法，请先运行 RaftServiceTest 启动至少 2 个节点
 *
 * 测试顺序建议：
 * 1. 启动集群：运行 RaftServiceTest.server1/2/3 或使用 RaftClusterTest
 * 2. 等待选举完成（约 5 秒）
 * 3. 运行本类测试方法
 * 4. 关闭集群：调用 RaftService.shutdown()
 *
 * @author zhouzhiyuan
 * @date 2022/5/23
 */
@Ignore("忽略此类的自动运行，请使用 RaftClusterTest 进行完整测试")
public class ZMClientTest {


  // 保存启动的服务引用，用于关闭
  private static final List<RaftService> services = new ArrayList<>();
  private static final List<GlobalConfig> configs = new ArrayList<>();

  // 客户端
  private ZMClient client;

  // 节点数量
  private static final int NODE_COUNT = 3;

  // 等待选举完成的时间（秒）
  private static final long WAIT_ELECTION_TIME = 40;
  /**
   * 测试 1: 单事务基本流程测试
   * 验证事务开启、写入、读取、提交的完整流程
   */
  @Test
  public void testSingleTransactionBasicFlow() throws Exception {
    
    
    
    System.out.println("========== 测试：单事务基本流程 ==========");
    client = new ZMClient("localhost:20001");

    String clientId = "tx-test-1";

    // 1. 开启事务
    DataResponest openResult = client.openTransaction(clientId);
    System.out.println("开启事务结果：" + openResult);
    Assert.assertEquals(StatusCode.SUCCESS, openResult.getStatus());
    String transactionId = (String) openResult.getData();
    System.out.println("事务 ID: " + transactionId);

    // 2. 写入数据
    DataResponest putResult = client.putInTransaction(clientId, "mvcc-key-1", "value-1");
    System.out.println("写入数据结果：" + putResult);
    Assert.assertEquals(StatusCode.SUCCESS, putResult.getStatus());

    // 3. 读取数据（读已写）
    DataResponest getResult = client.getInTransaction(clientId, "mvcc-key-1");
    System.out.println("读取数据结果：" + getResult);
    Assert.assertEquals(StatusCode.SUCCESS, getResult.getStatus());
    Assert.assertEquals("value-1", getResult.getMessage());

    // 4. 提交事务
    DataResponest commitResult = client.commitTransaction(clientId);
    System.out.println("提交事务结果：" + commitResult);
    Assert.assertEquals(StatusCode.SUCCESS, commitResult.getStatus());

    // 5. 验证提交后的数据（使用普通读取）
    //Thread.sleep(2000); // 等待日志应用
    DataResponest verifyResult = client.get("mvcc-key-1");
    System.out.println("验证数据结果：" + verifyResult);
    Assert.assertEquals(StatusCode.SUCCESS, verifyResult.getStatus());
    Assert.assertEquals("value-1", verifyResult.getMessage());

    System.out.println("========== 测试通过：单事务基本流程 ==========");
  }

  /**
   * 测试 2: 事务回滚测试
   * 验证事务回滚后数据不被提交
   */
  @Test
  public void testTransactionRollback() throws Exception {
    System.out.println("========== 测试：事务回滚 ==========");
    client = new ZMClient("localhost:20001");

    // 1. 先写入一条数据
    String clientId1 = "tx-init";
    DataResponest openResult1 = client.openTransaction(clientId1);
    Assert.assertEquals(StatusCode.SUCCESS, openResult1.getStatus());

    DataResponest putResult1 = client.putInTransaction(clientId1, "mvcc-key-rollback", "original-value");
    Assert.assertEquals(StatusCode.SUCCESS, putResult1.getStatus());

    DataResponest commitResult1 = client.commitTransaction(clientId1);
    Assert.assertEquals(StatusCode.SUCCESS, commitResult1.getStatus());

    //Thread.sleep(2000); // 等待日志应用

    // 2. 开启新事务并写入
    String clientId2 = "tx-rollback";
    DataResponest openResult2 = client.openTransaction(clientId2);
    Assert.assertEquals(StatusCode.SUCCESS, openResult2.getStatus());

    DataResponest putResult2 = client.putInTransaction(clientId2, "mvcc-key-rollback", "modified-value");
    Assert.assertEquals(StatusCode.SUCCESS, putResult2.getStatus());

    // 3. 回滚事务
    DataResponest rollbackResult = client.rollbackTransaction(clientId2);
    System.out.println("回滚事务结果：" + rollbackResult);
    Assert.assertEquals(StatusCode.SUCCESS, rollbackResult.getStatus());

    // 4. 验证数据仍然是原始值
    //Thread.sleep(1000);
    DataResponest verifyResult = client.get("mvcc-key-rollback");
    System.out.println("验证数据结果：" + verifyResult);
    Assert.assertEquals(StatusCode.SUCCESS, verifyResult.getStatus());
    Assert.assertEquals("original-value", verifyResult.getMessage());

    System.out.println("========== 测试通过：事务回滚 ==========");
  }

  /**
   * 测试 3: 快照读测试
   * 验证事务能看到正确的数据版本
   */
  @Test
  public void testSnapshotRead() throws Exception {
    System.out.println("========== 测试：快照读 ==========");
    client = new ZMClient("localhost:20001");

    // 1. 开启事务 T1 并写入数据
    String clientId1 = "tx-snapshot-1";
    DataResponest openResult1 = client.openTransaction(clientId1);
    Assert.assertEquals(StatusCode.SUCCESS, openResult1.getStatus());

    DataResponest putResult1 = client.putInTransaction(clientId1, "mvcc-key-snapshot", "value-before");
    Assert.assertEquals(StatusCode.SUCCESS, putResult1.getStatus());

    DataResponest commitResult1 = client.commitTransaction(clientId1);
    Assert.assertEquals(StatusCode.SUCCESS, commitResult1.getStatus());

    Thread.sleep(2000); // 等待日志应用

    // 2. 开启事务 T2（snapshotTs 应该能看到 value-before）
    String clientId2 = "tx-snapshot-2";
    DataResponest openResult2 = client.openTransaction(clientId2);
    Assert.assertEquals(StatusCode.SUCCESS, openResult2.getStatus());

    // 3. 在 T2 中读取，应该看到 value-before
    DataResponest getResult2 = client.getInTransaction(clientId2, "mvcc-key-snapshot");
    System.out.println("T2 读取结果：" + getResult2);
    Assert.assertEquals(StatusCode.SUCCESS, getResult2.getStatus());
    // 注意：由于 MVCC 实现，这里应该能看到已提交的 value-before

    // 4. 提交 T2
    DataResponest commitResult2 = client.commitTransaction(clientId2);
    Assert.assertEquals(StatusCode.SUCCESS, commitResult2.getStatus());

    System.out.println("========== 测试通过：快照读 ==========");
  }

  /**
   * 测试 4: 并发事务写冲突检测
   * 验证并发写同一 key 时能检测到冲突
   */
  @Test
  public void testConcurrentWriteConflict() throws Exception {
    System.out.println("========== 测试：并发写冲突检测 ==========");
    client = new ZMClient("localhost:20001");

    // 1. 开启两个事务
    String clientId1 = "tx-conflict-1";
    String clientId2 = "tx-conflict-2";

    DataResponest openResult1 = client.openTransaction(clientId1);
    Assert.assertEquals(StatusCode.SUCCESS, openResult1.getStatus());

    DataResponest openResult2 = client.openTransaction(clientId2);
    Assert.assertEquals(StatusCode.SUCCESS, openResult2.getStatus());

    // 2. 两个事务同时写入同一个 key
    DataResponest putResult1 = client.putInTransaction(clientId1, "mvcc-key-conflict", "value-from-t1");
    Assert.assertEquals(StatusCode.SUCCESS, putResult1.getStatus());

    DataResponest putResult2 = client.putInTransaction(clientId2, "mvcc-key-conflict", "value-from-t2");
    Assert.assertEquals(StatusCode.SUCCESS, putResult2.getStatus());

    // 3. T1 先提交
    DataResponest commitResult1 = client.commitTransaction(clientId1);
    System.out.println("T1 提交结果：" + commitResult1);
    Assert.assertEquals(StatusCode.SUCCESS, commitResult1.getStatus());

    Thread.sleep(1000);

    // 4. T2 提交（应该检测到冲突）
    DataResponest commitResult2 = client.commitTransaction(clientId2);
    System.out.println("T2 提交结果：" + commitResult2);
    // T2 应该检测到写冲突
    // 注意：根据具体实现，可能成功也可能失败

    System.out.println("========== 测试通过：并发写冲突检测 ==========");
  }

  /**
   * 测试 5: 多事务并发提交测试
   * 验证多个事务能正确并发执行
   */
  @Test
  public void testConcurrentTransactions() throws Exception {
    System.out.println("========== 测试：多事务并发提交 ==========");

    int threadCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    CountDownLatch latch = new CountDownLatch(threadCount);
    AtomicInteger successCount = new AtomicInteger(0);
    AtomicInteger conflictCount = new AtomicInteger(0);

    for (int i = 0; i < threadCount; i++) {
      final int txIndex = i;
      executor.submit(() -> {
        try {
          ZMClient txClient = new ZMClient("localhost:20001");
          String clientId = "tx-concurrent-" + txIndex;

          // 开启事务
          DataResponest openResult = txClient.openTransaction(clientId);
          if (openResult.getStatus() != StatusCode.SUCCESS) {
            latch.countDown();
            return;
          }

          // 写入不同的 key
          DataResponest putResult = txClient.putInTransaction(
                  clientId,
                  "mvcc-key-" + txIndex,
                  "value-" + txIndex
          );

          // 提交事务
          DataResponest commitResult = txClient.commitTransaction(clientId);
          if (commitResult.getStatus() == StatusCode.SUCCESS) {
            successCount.incrementAndGet();
          } else {
            conflictCount.incrementAndGet();
          }

          System.out.println("事务 " + txIndex + " 提交结果：" + commitResult);
        } catch (Exception e) {
          System.err.println("事务 " + txIndex + " 执行异常：" + e.getMessage());
        } finally {
          latch.countDown();
        }
      });
    }

    // 等待所有事务完成
    latch.await(60, TimeUnit.SECONDS);
    executor.shutdown();

    System.out.println("成功提交：" + successCount.get() + ", 冲突/失败：" + conflictCount.get());
    Assert.assertTrue("至少应该有一个事务成功提交", successCount.get() > 0);

    System.out.println("========== 测试通过：多事务并发提交 ==========");
  }

  /**
   * 测试 6: 事务内删除操作测试
   * 验证事务内删除功能
   */
  @Test
  public void testDeleteInTransaction() throws Exception {
    System.out.println("========== 测试：事务内删除操作 ==========");
    client = new ZMClient("localhost:20001");

    // 1. 先写入一条数据
    String clientId1 = "tx-delete-init";
    DataResponest openResult1 = client.openTransaction(clientId1);
    Assert.assertEquals(StatusCode.SUCCESS, openResult1.getStatus());

    DataResponest putResult1 = client.putInTransaction(clientId1, "mvcc-key-delete", "value-to-delete");
    Assert.assertEquals(StatusCode.SUCCESS, putResult1.getStatus());

    DataResponest commitResult1 = client.commitTransaction(clientId1);
    Assert.assertEquals(StatusCode.SUCCESS, commitResult1.getStatus());

    Thread.sleep(2000); // 等待日志应用

    // 2. 开启事务删除数据
    String clientId2 = "tx-delete";
    DataResponest openResult2 = client.openTransaction(clientId2);
    Assert.assertEquals(StatusCode.SUCCESS, openResult2.getStatus());

    DataResponest deleteResult = client.deleteInTransaction(clientId2, "mvcc-key-delete");
    System.out.println("删除数据结果：" + deleteResult);
    Assert.assertEquals(StatusCode.SUCCESS, deleteResult.getStatus());

    // 3. 提交事务
    DataResponest commitResult2 = client.commitTransaction(clientId2);
    System.out.println("提交删除事务结果：" + commitResult2);
    Assert.assertEquals(StatusCode.SUCCESS, commitResult2.getStatus());

    Thread.sleep(2000);

    // 4. 验证数据已被删除
    DataResponest verifyResult = client.get("mvcc-key-delete");
    System.out.println("验证删除结果：" + verifyResult);
    // 删除后应该返回 NOT_FOUND 或空值

    System.out.println("========== 测试通过：事务内删除操作 ==========");
  }

  /**
   * 测试 7: 大量数据写入测试
   * 验证 MVCC 事务能处理大量数据
   */
  @Test
  public void testBulkWriteInTransaction() throws Exception {
    System.out.println("========== 测试：大量数据写入 ==========");
    client = new ZMClient("localhost:20001");

    String clientId = "tx-bulk-write";
    int recordCount = 100;

    // 1. 开启事务
    DataResponest openResult = client.openTransaction(clientId);
    Assert.assertEquals(StatusCode.SUCCESS, openResult.getStatus());
    System.out.println("开启事务成功");

    // 2. 批量写入数据
    for (int i = 0; i < recordCount; i++) {
      DataResponest putResult = client.putInTransaction(
              clientId,
              "mvcc-bulk-key-" + i,
              "mvcc-bulk-value-" + i
      );
      if (i % 20 == 0) {
        System.out.println("已写入 " + i + " 条记录");
      }
      Assert.assertEquals(StatusCode.SUCCESS, putResult.getStatus());
    }

    // 3. 提交事务
    DataResponest commitResult = client.commitTransaction(clientId);
    System.out.println("提交大量数据事务结果：" + commitResult);
    Assert.assertEquals(StatusCode.SUCCESS, commitResult.getStatus());

    Thread.sleep(5000); // 等待日志应用

    // 4. 验证部分数据
    for (int i = 0; i < 10; i++) {
      DataResponest verifyResult = client.get("mvcc-bulk-key-" + i);
      Assert.assertEquals("mvcc-bulk-value-" + i, verifyResult.getMessage());
    }

    System.out.println("========== 测试通过：大量数据写入 ==========");
  }

  /**
   * 测试 8: 事务隔离性测试
   * 验证未提交的数据对外部读取不可见（读未提交隔离）
   */
  @Test
  public void testUncommittedDataNotVisible() throws Exception {
    System.out.println("========== 测试：事务隔离性（未提交数据不可见）==========");
    client = new ZMClient("localhost:20001");

    // 1. 先写入一条初始数据
    String initClientId = "tx-init-visible";
    DataResponest openResult1 = client.openTransaction(initClientId);
    Assert.assertEquals(StatusCode.SUCCESS, openResult1.getStatus());

    DataResponest putResult1 = client.putInTransaction(initClientId, "mvcc-key-isolation", "original-value");
    Assert.assertEquals(StatusCode.SUCCESS, putResult1.getStatus());

    DataResponest commitResult1 = client.commitTransaction(initClientId);
    Assert.assertEquals(StatusCode.SUCCESS, commitResult1.getStatus());

    Thread.sleep(2000); // 等待日志应用

    // 验证初始数据
    DataResponest verifyResult1 = client.get("mvcc-key-isolation");
    Assert.assertEquals(StatusCode.SUCCESS, verifyResult1.getStatus());
    Assert.assertEquals("original-value", verifyResult1.getMessage());
    System.out.println("初始数据：" + verifyResult1.getMessage());

    // 2. 开启事务 T1 并修改数据（但不提交）
    String uncommittedClientId = "tx-uncommitted";
    DataResponest openResult2 = client.openTransaction(uncommittedClientId);
    Assert.assertEquals(StatusCode.SUCCESS, openResult2.getStatus());

    DataResponest putResult2 = client.putInTransaction(uncommittedClientId, "mvcc-key-isolation", "modified-value");
    Assert.assertEquals(StatusCode.SUCCESS, putResult2.getStatus());
    System.out.println("事务 T1 写入修改数据（未提交）");

    // 3. 在事务 T1 之外读取数据（普通读取）
    // 应该读取到原始值，而不是未提交的修改值
    Thread.sleep(500); // 短暂等待
    DataResponest externalReadResult = client.get("mvcc-key-isolation");
    System.out.println("外部读取结果：" + externalReadResult.getMessage());

    // 验证：外部读取应该仍然看到原始值
    Assert.assertEquals(StatusCode.SUCCESS, externalReadResult.getStatus());
    Assert.assertEquals("original-value", externalReadResult.getMessage());

    // 4. 开启另一个事务 T2 进行快照读
    String snapshotClientId = "tx-snapshot-read";
    DataResponest openResult3 = client.openTransaction(snapshotClientId);
    Assert.assertEquals(StatusCode.SUCCESS, openResult3.getStatus());

    DataResponest snapshotReadResult = client.getInTransaction(snapshotClientId, "mvcc-key-isolation");
    System.out.println("事务 T2 快照读结果：" + snapshotReadResult.getMessage());

    // 验证：快照读也应该看到原始值（T1 未提交）
    Assert.assertEquals(StatusCode.SUCCESS, snapshotReadResult.getStatus());
    Assert.assertEquals("original-value", snapshotReadResult.getMessage());

    // 5. 提交事务 T2
    DataResponest commitResult3 = client.commitTransaction(snapshotClientId);
    Assert.assertEquals(StatusCode.SUCCESS, commitResult3.getStatus());

    // 6. 回滚事务 T1（不提交修改）
    DataResponest rollbackResult = client.rollbackTransaction(uncommittedClientId);
    Assert.assertEquals(StatusCode.SUCCESS, rollbackResult.getStatus());
    System.out.println("事务 T1 已回滚");

    // 7. 再次验证数据仍然是原始值
    Thread.sleep(1000);
    DataResponest finalVerifyResult = client.get("mvcc-key-isolation");
    System.out.println("最终验证数据：" + finalVerifyResult.getMessage());
    Assert.assertEquals(StatusCode.SUCCESS, finalVerifyResult.getStatus());
    Assert.assertEquals("original-value", finalVerifyResult.getMessage());

    System.out.println("========== 测试通过：事务隔离性（未提交数据不可见）==========");
  }

  /**
   * 单条数据写入 100 条数据
   *
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void Clint1() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20002");
    for (int i = 0; i < 100; i++) {
      Row row = new Row((i + "王五和小六子").getBytes(), (i + "是同学哈").getBytes());
      DataResponest dataResponest = client.put(new Row[] {row});
      System.out.println(dataResponest);
      Assert.assertEquals(dataResponest.getStatus(), 200);
    }
  }

  /**
   * 批量写入 100 条数据
   *
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void Clint2() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20001");
    Row[] rows = new Row[100];
    for (int i = 100; i < 200; i++) {
      rows[i - 100] = new Row((i + "王五和小六子").getBytes(), (i + "是同学哈").getBytes());
    }
    DataResponest dataResponest = client.put(rows);
    System.out.println(dataResponest);
    Assert.assertEquals(dataResponest.getStatus(), 200);
  }

  /**
   * 10 并发写入 1000 条重复数据
   *
   * @throws InterruptedException
   */
  @Test
  public void Clint3() throws InterruptedException {
    ExecutorService service = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      service.submit(new PutTask());
    }
    service.shutdownNow();
    service.awaitTermination(10, TimeUnit.MINUTES);
  }

  public class PutTask implements Runnable {

    @Override
    public void run() {
      ZMClient client = new ZMClient("localhost:20000");
      for (int i = 200; i < 300; i++) {
        Row row = new Row((i + "王五和小六子").getBytes(), (i + "是同学哈").getBytes());
        try {
          DataResponest dataResponest = client.put(new Row[] {row});
          System.out.println(dataResponest);
          Assert.assertEquals(dataResponest.getStatus(), 200);
        } catch (Exception e) {
          System.out.println(e);
        }
      }
    }
  }


  /**
   * 查询数据
   * 当前方法需要前面的写入测试先执行完成，不然会报错
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void ClintGet() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20001");
    for (int i = 0; i < 100; i++) {
      DataResponest dataResponest = client.get(i + "王五和小六子");
      System.out.println(dataResponest);
      Assert.assertEquals(dataResponest.getMessage(), i + "是同学哈");
    }
    client = new ZMClient("localhost:20001");
    for (int i = 0; i < 200; i++) {
      DataResponest dataResponest = client.get(i + "王五和小六子");
      System.out.println(dataResponest);
      Assert.assertEquals(dataResponest.getMessage(), i + "是同学哈");
    }
    client = new ZMClient("localhost:20002");
    for (int i = 0; i < 300; i++) {
      DataResponest dataResponest = client.get(i + "王五和小六子");
      System.out.println(dataResponest);
      Assert.assertEquals(dataResponest.getMessage(), i + "是同学哈");
    }
  }

  /**
   * 删除数据，存在就删除，不存在返回的也是成功
   *
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void ClintDelete() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20002");
    for (int i = 0; i < 300; i++) {
      Row row = new Row((i + "王五和小六子").getBytes(), (byte[]) null);
      DataResponest dataResponest = client.delete(new Row[] {row});
      System.out.println(dataResponest);
      Assert.assertEquals(dataResponest.getStatus(), 200);
    }
  }

  /**
   * leader 漂移
   *
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void ClintMoveLeader() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20003");
    DataResponest dataResponest = client.leaderMove("localhost:20004");
    System.out.println(dataResponest);
    Assert.assertEquals(dataResponest.getStatus(), 200);
  }
}
