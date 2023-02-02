package com.zhiyuan.zm;

import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.Row;
import com.zhiyuan.zm.raft.rpc.DefaultRpcClient;
import org.junit.Test;

import com.alipay.remoting.exception.RemotingException;
import com.zhiyuan.zm.raft.rpc.ZMClient;

/**
 *@author zhouzhiyuan
 *@date 2022/5/23
 */
public class ZMClientTest {


  /**
   * 写入数据
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void Clint1() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20002");
    for (int i = 0; i < 100; i++) {
      Row row = new Row((i+"王五和小六子").getBytes(),(i+"是同学哈").getBytes());
      DataResponest dataResponest = client.put( new Row[]{row});
      System.out.println(dataResponest);
    }
  }

  @Test
  public void Clint2() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20001");
    for (int i = 100; i < 200; i++) {
      Row row = new Row((i+"王五和小六子").getBytes(),(i+"是同学哈").getBytes());
      DataResponest dataResponest = client.put( new Row[]{row});
      System.out.println(dataResponest);
    }
  }

  @Test
  public void Clint3() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20000");
    for (int i = 200; i < 300; i++) {
      Row row = new Row((i+"王五和小六子").getBytes(),(i+"是同学哈").getBytes());
      DataResponest dataResponest = client.put( new Row[]{row});
      System.out.println(dataResponest);
    }
  }

  /**
   * 查询数据
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void ClintGet() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20000");
    for (int i = 0; i < 300; i++) {
      DataResponest dataResponest = client.get( i+"王五和小六子");
      System.out.println(dataResponest);

    }
  }

  /**
   * 查询数据
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void ClintGet2() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20001");
    for (int i = 0; i < 300; i++) {
      DataResponest dataResponest = client.get(i+"王五和小六子");
      System.out.println(dataResponest);

    }
  }

  /**
   * 查询数据
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void ClintGet3() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20002");
    for (int i = 0; i < 300; i++) {
      DataResponest dataResponest = client.get(i+"王五和小六子");
      System.out.println(dataResponest);

    }
  }

  /**
   * 删除数据
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void ClintDelete() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20002");
    for (int i = 0; i < 300; i++) {
      Row row = new Row((i+"王五和小六子").getBytes(),null);
      DataResponest dataResponest = client.delete(new Row[]{row});
      System.out.println(dataResponest);
    }
  }

  /**
   * 删除数据
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void ClintMoveLeader() throws RemotingException, InterruptedException {
    ZMClient client = new ZMClient("localhost:20000");
    DataResponest dataResponest = client.leaderMove("localhost:20003");
    System.out.println(dataResponest);
  }

}
