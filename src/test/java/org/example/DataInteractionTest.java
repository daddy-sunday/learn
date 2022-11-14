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
    Row row = new Row("王五".getBytes(),"103级".getBytes());
      DataResponest dataResponest = DefaultRpcClient.put("localhost:20002", new Row[]{row}, 10000);
      System.out.println(dataResponest);
    System.out.println(DefaultRpcClient.get("localhost:20001", "王五", 10000));
  }

  /**
   * 查询数据
   * @throws RemotingException
   * @throws InterruptedException
   */
  @Test
  public void ClintGet() throws RemotingException, InterruptedException {
    DataResponest dataResponest = DefaultRpcClient.get("localhost:20001", "王五", 10000);
    System.out.println(dataResponest);
  }


}
