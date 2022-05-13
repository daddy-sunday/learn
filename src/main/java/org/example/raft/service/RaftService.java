package org.example.raft.service;

import org.example.conf.GlobalConfig;
import org.example.raft.dto.LogEntries;
import org.example.raft.persistence.DefaultSaveDataImpl;
import org.example.raft.persistence.DefaultSaveLogImpl;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.RoleStatus;
import org.example.raft.rpc.DataRpcHandler;
import org.example.raft.rpc.DefaultRpcServer;
import org.example.raft.rpc.RaftRpcHandler;
import org.example.raft.util.ByteUtil;
import org.example.raft.util.RaftUtil;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

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
    SaveData saveData = new DefaultSaveDataImpl(conf);
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
    //todo  startKey ,endKey 初始化
    int groupId = raftStatus.getGroupId();
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

    byte[] bytes = saveLog.getBytes(RaftUtil.generateRaftInitKey(groupId));
    if (bytes == null) {
      //写入一个空字节
      WriteBatch writeBatch = new WriteBatch();
      writeBatch.put(RaftUtil.generateRaftInitKey(groupId), new byte[] {});
      //初始化一条log
      writeBatch.put(RaftUtil.generateLogKey(groupId, 5L),
          JSON.toJSONBytes(new LogEntries(RaftUtil.INIT_LOG_INDEX, RaftUtil.INIT_TERM, "")));
      raftStatus.setCommitIndex(RaftUtil.INIT_LOG_INDEX);
      raftStatus.setCurrentTerm(RaftUtil.INIT_TERM);
      raftStatus.setLastApplied(RaftUtil.INIT_LOG_INDEX);
    } else {
      LogEntries maxLog = saveLog.getMaxLog(RaftUtil.generateLogKey(groupId, Long.MAX_VALUE));
      raftStatus.setCommitIndex(maxLog.getLogIndex());
      raftStatus.setCurrentTerm(maxLog.getTerm());
      raftStatus.setLastApplied(
          ByteUtil.bytesToLong(saveLog.getBytes(RaftUtil.generateApplyLogKey(groupId))));
    }

    return raftStatus;
  }
}
