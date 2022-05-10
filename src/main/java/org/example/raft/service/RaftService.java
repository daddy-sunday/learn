package org.example.raft.service;

import org.example.conf.GlobalConfig;
import org.example.raft.dto.AddLog;
import org.example.raft.persistence.DefaultSaveData;
import org.example.raft.persistence.DefaultSaveLogImpl;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.RoleStatus;
import org.example.raft.rpc.DataRpcHandler;
import org.example.raft.rpc.DefaultRpcServer;
import org.example.raft.rpc.RaftRpcHandler;
import org.example.raft.util.ByteUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class RaftService {

  private static final Logger LOG = LoggerFactory.getLogger(RaftService.class);


  public void start(GlobalConfig conf) throws RocksDBException {
    LOG.info("global conf: " + conf);
    //存储和初始化
    SaveLog saveLog = new DefaultSaveLogImpl(conf);
    SaveData saveData = new DefaultSaveData(conf);
    RaftStatus raftStatus = initRaftStatus(saveData, saveLog, conf);
    //处理逻辑
    RoleService roleService = new RoleService(saveData, conf, raftStatus, new RoleStatus(), saveLog);
    RaftRpcHandler raftRpcHandler = new RaftRpcHandler(roleService);
    DataRpcHandler dataRpcHandler = new DataRpcHandler(roleService);
    //网络通信
    DefaultRpcServer server = new DefaultRpcServer(conf, raftRpcHandler, dataRpcHandler);
    server.start();
    roleService.startWork();
  }

  /**
   * 集群状态初始化
   */
  private RaftStatus initRaftStatus(SaveData data, SaveLog saveLog, GlobalConfig conf) throws RocksDBException {
    RaftStatus raftStatus = new RaftStatus();
    String[] split = conf.getOtherNode().split(",");
    String currentNode = conf.getCurrentNode();
    for (String s : split) {
      if (!s.equals(currentNode)) {
        raftStatus.getAllMembers().add(s);
        raftStatus.getValidMembers().add(s);
      }
    }
    raftStatus.setLocalAddress(currentNode);
    raftStatus.setPersonelNum(split.length);

    AddLog maxLog = saveLog.getMaxLog();
    if (maxLog != null) {
      raftStatus.setCommitIndex(maxLog.getLogIndex());
      raftStatus.setCurrentTerm(maxLog.getTerm());
    }
    raftStatus.setLastApplied(1);

    // todo  groupId raftStatus.setGroupId();
    // 协议第一次运行时,需要的初始数据
    AddLog addLog = new AddLog(0, 0, null, 1, 1,
        null, 0);
    saveLog.saveLog(ByteUtil.concatLogId(raftStatus.getGroupId(), 0), addLog);

    return raftStatus;
  }
}
