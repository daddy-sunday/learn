package com.zhiyuan.zm;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.service.RaftService;
import org.junit.Test;
import org.rocksdb.RocksDBException;

/**
 *@author zhouzhiyuan
 *@date 2021/12/10
 */
public class RaftServiceTest {
  /**
   * 启动一个存储节点
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
    globalConfig.setLogPath("D:\\tmp\\raft\\log2");
    globalConfig.setDataPath("D:\\tmp\\raft\\data2");
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004");
    globalConfig.setPort(20001);
    globalConfig.setCurrentNode("localhost:20001");
    RaftService raftService = new RaftService();
    raftService.start(globalConfig);
  }

  @Test
  public void server3() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setLogPath("D:\\tmp\\raft\\log3");
    globalConfig.setDataPath("D:\\tmp\\raft\\data3");
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004");
    globalConfig.setPort(20002);
    globalConfig.setCurrentNode("localhost:20002");
    RaftService raftService = new RaftService();
    raftService.start(globalConfig);
  }

  @Test
  public void server4() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setLogPath("D:\\tmp\\raft\\log4");
    globalConfig.setDataPath("D:\\tmp\\raft\\data4");
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004");
    globalConfig.setPort(20003);
    globalConfig.setCurrentNode("localhost:20003");
    RaftService raftService = new RaftService();
    raftService.start(globalConfig);
  }

  @Test
  public void server5() throws InterruptedException, RocksDBException {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setLogPath("D:\\tmp\\raft\\log5");
    globalConfig.setDataPath("D:\\tmp\\raft\\data5");
    globalConfig.setOtherNode("localhost:20000,localhost:20001,localhost:20002,localhost:20003,localhost:20004");
    globalConfig.setPort(20004);
    globalConfig.setCurrentNode("localhost:20004");
    RaftService raftService = new RaftService();
    raftService.start(globalConfig);
  }



}
