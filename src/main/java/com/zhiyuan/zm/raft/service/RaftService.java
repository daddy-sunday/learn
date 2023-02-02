package com.zhiyuan.zm.raft.service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.extend.UserWork;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.TaskMaterial;
import com.zhiyuan.zm.raft.persistence.DefaultSaveDataImpl;
import com.zhiyuan.zm.raft.persistence.DefaultSaveLogImpl;
import com.zhiyuan.zm.raft.persistence.SaveData;
import com.zhiyuan.zm.raft.persistence.SaveLog;
import com.zhiyuan.zm.raft.role.RoleStatus;
import com.zhiyuan.zm.raft.role.active.ApplyLogTask;
import com.zhiyuan.zm.raft.role.active.SaveLogTask;
import com.zhiyuan.zm.raft.rpc.DataRpcHandler;
import com.zhiyuan.zm.raft.rpc.DefaultRpcServer;
import com.zhiyuan.zm.raft.rpc.RaftRpcHandler;
import com.zhiyuan.zm.raft.util.ByteUtil;
import com.zhiyuan.zm.raft.util.RaftUtil;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 * @author zhouzhiyuan
 * @date 2021/10/22
 */
public class RaftService {

  private static final Logger LOG = LoggerFactory.getLogger(RaftService.class);

  public void start(GlobalConfig conf) throws RocksDBException {
    start(conf, null);
  }

  public void start(GlobalConfig conf, UserWork userWork) throws RocksDBException {
    LOG.info("global conf: " + conf);
    //基础组件
    SaveLog saveLog = new DefaultSaveLogImpl(conf);
    SaveData saveData = new DefaultSaveDataImpl(conf);
    RaftStatus raftStatus = initRaftStatus(saveData, saveLog, conf);
    raftStatus.initDebug();
    BlockingQueue<LogEntries[]> applyLogQueue = new LinkedBlockingDeque<>(1000);
    BlockingQueue<TaskMaterial> saveLogQueue = new LinkedBlockingDeque<>(1000);
    ApplyLogTask applyLogTask = new ApplyLogTask(applyLogQueue, raftStatus, saveData, saveLog, conf);
    SaveLogTask saveLogTask = new SaveLogTask(saveLogQueue, raftStatus, saveLog, conf);

    //核心处理逻辑
    RoleService roleService = new RoleService(saveData, conf, raftStatus, new RoleStatus(), saveLog, applyLogQueue,
        saveLogQueue, saveLogTask);
    roleService.setUserWork(userWork);
    RaftRpcHandler raftRpcHandler = new RaftRpcHandler(roleService);
    DataRpcHandler dataRpcHandler = new DataRpcHandler(roleService);

    //网络通信
    DefaultRpcServer server = new DefaultRpcServer(conf, raftRpcHandler, dataRpcHandler);
    server.start();
    applyLogTask.start();
    saveLogTask.start();
    roleService.startWork();
  }


  /**
   * 集群状态初始化
   */
  private RaftStatus initRaftStatus(SaveData saveData, SaveLog saveLog, GlobalConfig conf) throws RocksDBException {

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
      writeBatch.put(RaftUtil.generateLogKey(groupId, RaftUtil.INIT_LOG_INDEX),
          JSON.toJSONBytes(new LogEntries(RaftUtil.INIT_LOG_INDEX, RaftUtil.INIT_TERM, "")));
      raftStatus.setCurrentTerm(RaftUtil.INIT_TERM);
      raftStatus.setAppliedIndex(RaftUtil.INIT_LOG_INDEX);
      raftStatus.setLastTimeLogIndex(RaftUtil.INIT_LOG_INDEX);
      raftStatus.setLastTimeTerm(RaftUtil.INIT_TERM);
      saveLog.writBatch(writeBatch);
      saveData.put(RaftUtil.generateApplyLogKey(groupId), ByteUtil.longToBytes(RaftUtil.INIT_LOG_INDEX));
    } else {
      LogEntries maxLog = saveLog.getMaxLog(RaftUtil.generateLogKey(groupId, Long.MAX_VALUE));
      raftStatus.setCurrentTerm(maxLog.getTerm());
      raftStatus.setLastTimeLogIndex(maxLog.getLogIndex());
      raftStatus.setLastTimeTerm(maxLog.getTerm());
      byte[] appliedLogIndex = saveData.getValue(RaftUtil.generateApplyLogKey(groupId));
      if (appliedLogIndex == null) {
        //上一次初始化失败时才有可能会走到这里
        if (maxLog.getLogIndex() != RaftUtil.INIT_LOG_INDEX) {
          LOG.error("出现了未知的情况，程序必须退出");
          System.exit(100);
        }
        saveData.put(RaftUtil.generateApplyLogKey(groupId), ByteUtil.longToBytes(RaftUtil.INIT_LOG_INDEX));
        raftStatus.setAppliedIndex(RaftUtil.INIT_LOG_INDEX);
      } else {
        raftStatus.setAppliedIndex(
            ByteUtil.bytesToLong(appliedLogIndex));
      }
    }

    return raftStatus;
  }
}
