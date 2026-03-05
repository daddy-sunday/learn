package com.zhiyuan.zm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.Row;
import com.zhiyuan.zm.raft.rpc.DefaultRpcClient;

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
