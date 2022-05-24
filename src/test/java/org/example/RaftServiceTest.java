package org.example;

import org.example.conf.GlobalConfig;
import org.example.raft.service.RaftService;
import org.junit.Test;
import org.rocksdb.RocksDBException;

/**
 *@author zhouzhiyuan
 *@date 2021/12/10
 */
public class RaftServiceTest {
  /**
   * Rigorous Test :-)
   */
  @Test
  public void server1() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004");
    globalConfig.setPort(20000);
    globalConfig.setCurrentNode("localhost:20000");
    RaftService raftService = new RaftService();
    raftService.start(globalConfig);
  }

  @Test
  public void server2() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setLogPath("C:\\Users\\zhouz\\Desktop\\raft\\log2");
    globalConfig.setDataPath("C:\\Users\\zhouz\\Desktop\\raft\\data2");
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004");
    globalConfig.setPort(20001);
    globalConfig.setCurrentNode("localhost:20001");
    RaftService raftService = new RaftService();
    raftService.start(globalConfig);
  }

  @Test
  public void server3() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setLogPath("C:\\Users\\zhouz\\Desktop\\raft\\log3");
    globalConfig.setDataPath("C:\\Users\\zhouz\\Desktop\\raft\\data3");
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004");
    globalConfig.setPort(20002);
    globalConfig.setCurrentNode("localhost:20002");
    RaftService raftService = new RaftService();
    raftService.start(globalConfig);
  }

  @Test
  public void server4() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setLogPath("C:\\Users\\zhouz\\Desktop\\raft\\log4");
    globalConfig.setDataPath("C:\\Users\\zhouz\\Desktop\\raft\\data4");
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004");
    globalConfig.setPort(20003);
    globalConfig.setCurrentNode("localhost:20003");
    RaftService raftService = new RaftService();
    raftService.start(globalConfig);
  }

  @Test
  public void server5() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setLogPath("C:\\Users\\zhouz\\Desktop\\raft\\log5");
    globalConfig.setDataPath("C:\\Users\\zhouz\\Desktop\\raft\\data5");
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004");
    globalConfig.setPort(20004);
    globalConfig.setCurrentNode("localhost:20004");
    RaftService raftService = new RaftService();
    raftService.start(globalConfig);
  }



}
