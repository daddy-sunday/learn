package org.example;

import org.example.raft.constant.DataOperationType;
import org.example.raft.constant.MessageType;
import org.example.raft.dto.Command;
import org.example.raft.dto.DataRequest;
import org.example.raft.dto.DataResponest;
import org.example.raft.dto.Row;
import org.example.raft.rpc.DefaultRpcClient;
import org.junit.Test;

import com.alibaba.fastjson.JSON;
import com.alipay.remoting.exception.RemotingException;

/**
 *@author zhouzhiyuan
 *@date 2022/5/23
 */
public class DataInteractionTest {


  /**
   * 写入数据并查询
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void Clint1() throws RemotingException, InterruptedException {
    for (int i = 0; i < 100; i++) {
      Row row = new Row((i+"王五和小六子").getBytes(),(i+"同学哈哈").getBytes());
      DataResponest dataResponest = DefaultRpcClient.put("localhost:20002", new Row[]{row}, 10000);
      System.out.println(dataResponest);
    }
  }

  @Test
  public void Clint2() throws RemotingException, InterruptedException {
    for (int i = 100; i < 200; i++) {
      Row row = new Row((i+"王五和小六子").getBytes(),(i+"同学哈哈").getBytes());
      DataResponest dataResponest = DefaultRpcClient.put("localhost:20000", new Row[]{row}, 10000);
      System.out.println(dataResponest);
    }
  }

  @Test
  public void Clint3() throws RemotingException, InterruptedException {
    for (int i = 200; i < 300; i++) {
      Row row = new Row((i+"王五和小六子").getBytes(),(i+"同学哈哈").getBytes());
      DataResponest dataResponest = DefaultRpcClient.put("localhost:20000", new Row[]{row}, 10000);
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
    for (int i = 0; i < 50; i++) {
      DataResponest dataResponest = DefaultRpcClient.get("localhost:20000", (i+"王五和小六子"), 10000);
      System.out.println(dataResponest);

    }
  }

  @Test
  public void ClintDelete() throws RemotingException, InterruptedException {
    for (int i = 0; i < 100; i++) {
      Row row = new Row((i+"王五和小六子").getBytes(),(i+"同学哈哈").getBytes());
      DataResponest dataResponest = DefaultRpcClient.delete("localhost:20004", new Row[]{row}, 10000);
      System.out.println(dataResponest);
    }
  }


}
