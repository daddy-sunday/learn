package org.example;

import org.example.raft.constant.DataOperationType;
import org.example.raft.dto.DataRequest;
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
    Row row = new Row("wo".getBytes(),"shi".getBytes());
    request.setMessage(JSON.toJSONString(row));
    request.setType(DataOperationType.INSERT);
    DefaultRpcClient.dataRequest("localhost:20002",request);
  }

}
