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
  @Test
  public void Clint() throws RemotingException, InterruptedException {
    DataRequest request = new DataRequest();
    Row row = new Row("wo1".getBytes(),"shi1".getBytes());
    Command command = new Command(DataOperationType.INSERT,new Row[]{row});
    request.setMessage(JSON.toJSONString(command));
    request.setType(MessageType.SET);
    DataResponest dataResponest = DefaultRpcClient.dataRequest("localhost:20002", request, 100000);
    System.out.println(dataResponest);
  }

}