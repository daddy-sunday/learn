package com.zhiyuan.zm;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.Row;
import com.zhiyuan.zm.raft.rpc.DefaultRpcClient;

import org.junit.Assert;
import org.junit.Test;

import com.alipay.remoting.exception.RemotingException;
import com.zhiyuan.zm.raft.rpc.ZMClient;

/**
 * 注意：在执行这个类中方法时需要保证 leader节点已经选举完成，否则会返回错误码
 *
 * @author zhouzhiyuan
 * @date 2022/5/23
 */
public class ZMClientTest {



  /**
   * 单条数据写入 100条数据
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
   * 批量写入100条数据
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
   * 10并发写入 1000条重复数据
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
   *当前方法需要上面的三个方法先执行完成，不然会报错
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
   * 删除数据,存在就删除，不存在返回的也是成功
   *
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void ClintDelete() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20002");
    for (int i = 0; i < 300; i++) {
      Row row = new Row((i + "王五和小六子").getBytes(), null);
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
