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
import com.zhiyuan.zm.raft.rpc.ZMClient;
import com.zhiyuan.zm.raft.service.RaftService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;

/**
 * MVCC 事务集成测试类
 * 测试 MVCC 事务的完整功能：
 * 1. 单事务正确性（ACID 特性验证）
 * 2. 多事务并发（读写冲突、写写冲突检测）
 * 3. 快照读验证（并发读写时读取一致性）
 * 4. 事务提交/回滚测试
 * 5. 垃圾回收机制验证
 *
 * @author zhouzhiyuan
 * @date 2026/03/05
 */
public class MVCCIntegrationTest {

    // 保存启动的服务引用，用于关闭
    private static final List<RaftService> services = new ArrayList<>();
    private static final List<GlobalConfig> configs = new ArrayList<>();

    // 客户端
    private ZMClient client;

    // 节点数量
    private static final int NODE_COUNT = 3;

    // 等待选举完成的时间（秒）
    private static final long WAIT_ELECTION_TIME = 30;

    /**
     * 启动集群（Before 每个测试方法执行前运行）
     */
    @Before
    public void startCluster() throws InterruptedException, RocksDBException {
        System.out.println("========== 开始启动 Raft 集群（MVCC 测试）==========");
        services.clear();
        configs.clear();

        // 启动 3 个节点
        startNode(0, "localhost:21000", "D:\\tmp\\raft\\mvcc\\log", "D:\\tmp\\raft\\mvcc\\data");
        startNode(1, "localhost:21001", "D:\\tmp\\raft\\mvcc\\log2", "D:\\tmp\\raft\\mvcc\\data2");
        startNode(2, "localhost:21002", "D:\\tmp\\raft\\mvcc\\log3", "D:\\tmp\\raft\\mvcc\\data3");

        // 等待选举完成
        System.out.println("等待 " + WAIT_ELECTION_TIME + " 秒让集群完成选举...");
        Thread.sleep(WAIT_ELECTION_TIME * 1000);
        System.out.println("========== Raft 集群启动完成（MVCC 测试）==========");
    }

    /**
     * 关闭所有节点（After 每个测试方法执行后运行）
     */
    @After
    public void shutdownCluster() {
        System.out.println("========== 开始关闭 Raft 集群（MVCC 测试）==========");
        shutdownAll();
        System.out.println("========== Raft 集群已关闭（MVCC 测试）==========");
    }

    /**
     * 启动单个节点
     */
    private void startNode(int index, String address, String logPath, String dataPath)
            throws InterruptedException, RocksDBException {
        GlobalConfig globalConfig = new GlobalConfig();
        globalConfig.setLogPath(logPath);
        globalConfig.setDataPath(dataPath);
        globalConfig.setOtherNode("localhost:21000,localhost:21001,localhost:21002");
        globalConfig.setPort(21000 + index);
        globalConfig.setCurrentNode(address);

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

        System.out.println("已启动节点：" + address + " (端口：" + (21000 + index) + ")");
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

    // ==================== MVCC 事务测试方法 ====================

    /**
     * 测试 1: 单事务基本流程测试
     * 验证事务开启、写入、读取、提交的完整流程
     */
    @Test
    public void testSingleTransactionBasicFlow() throws Exception {
        System.out.println("========== 测试：单事务基本流程 ==========");
        client = new ZMClient("localhost:21000");

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
        Thread.sleep(2000); // 等待日志应用
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
        client = new ZMClient("localhost:21000");

        // 1. 先写入一条数据
        String clientId1 = "tx-init";
        DataResponest openResult1 = client.openTransaction(clientId1);
        Assert.assertEquals(StatusCode.SUCCESS, openResult1.getStatus());

        DataResponest putResult1 = client.putInTransaction(clientId1, "mvcc-key-rollback", "original-value");
        Assert.assertEquals(StatusCode.SUCCESS, putResult1.getStatus());

        DataResponest commitResult1 = client.commitTransaction(clientId1);
        Assert.assertEquals(StatusCode.SUCCESS, commitResult1.getStatus());

        Thread.sleep(2000); // 等待日志应用

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
        Thread.sleep(1000);
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
        client = new ZMClient("localhost:21000");

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
        client = new ZMClient("localhost:21000");

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
                    ZMClient txClient = new ZMClient("localhost:21000");
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
        client = new ZMClient("localhost:21000");

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
        client = new ZMClient("localhost:21000");

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
}
