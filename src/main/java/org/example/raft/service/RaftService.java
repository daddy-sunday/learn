package org.example.raft.service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import org.example.conf.GlobalConfig;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.TaskMaterial;
import org.example.raft.persistence.DefaultSaveDataImpl;
import org.example.raft.persistence.DefaultSaveLogImpl;
import org.example.raft.persistence.SaveData;
import org.example.raft.persistence.SaveIterator;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.RoleStatus;
import org.example.raft.role.active.ApplyLogTask;
import org.example.raft.role.active.SaveLogTask;
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
    //基础组件
    SaveLog saveLog = new DefaultSaveLogImpl(conf);
    SaveData saveData = new DefaultSaveDataImpl(conf);
    RaftStatus raftStatus = initRaftStatus(saveData, saveLog, conf);
    BlockingQueue<LogEntries[]> applyLogQueue = new LinkedBlockingDeque<>(1000);
    BlockingQueue<TaskMaterial> saveLogQueue = new LinkedBlockingDeque<>(1000);
    ApplyLogTask applyLogTask = new ApplyLogTask(applyLogQueue, raftStatus, saveData,conf);
    SaveLogTask saveLogTask = new SaveLogTask(saveLogQueue, raftStatus, saveLog,conf);

    //核心处理逻辑
    RoleService roleService = new RoleService(saveData, conf, raftStatus, new RoleStatus(), saveLog, applyLogQueue,
        saveLogQueue,saveLogTask);
    RaftRpcHandler raftRpcHandler = new RaftRpcHandler(roleService);
    DataRpcHandler dataRpcHandler = new DataRpcHandler(roleService);
    //网络通信
    DefaultRpcServer server = new DefaultRpcServer(conf, raftRpcHandler, dataRpcHandler);
    server.start();
    applyLogTask.start();
    saveLogTask.start();
    roleService.startWork();
    try {
      Thread.sleep(1000*60*60);
    } catch (InterruptedException ignored) {
    }
  }


  private void init(RaftStatus raftStatus,SaveLog saveLog,SaveData saveData) throws RocksDBException {
    long commitIndex = raftStatus.getCommitIndex();
    long lastApplied = raftStatus.getLastApplied();
    byte[] bytes = RaftUtil.generateDataKey(raftStatus.getGroupId());
    if (lastApplied < commitIndex) {
      SaveIterator scan = saveLog.scan(RaftUtil.generateLogKey(raftStatus.getGroupId(), lastApplied),
          RaftUtil.generateLogKey(raftStatus.getGroupId(), lastApplied));
      //todo 优化 数据量很大时有内存溢出的风险
      WriteBatch writeBatch = new WriteBatch();
      for (scan.seek();scan.valied();scan.next()){
        byte[] value = scan.getValue();
        LogEntries[] entries = JSON.parseObject(value,LogEntries[].class);
        saveData.assembleData(writeBatch,entries,bytes);
      }
      //提交 applied id  随批提交应用日志记录，保证原子性
      writeBatch.put(RaftUtil.generateApplyLogKey(raftStatus.getGroupId()), ByteUtil.longToBytes(commitIndex));
      saveData.writBatch(writeBatch);
      LOG.info("应用日志完成："+lastApplied+" -> "+ commitIndex);
    }
    //读取commitLogIndex
    //读取appliedLogIndex
    //应用被提交的log
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
      raftStatus.setCommitIndex(RaftUtil.INIT_LOG_INDEX);
      raftStatus.setCurrentTerm(RaftUtil.INIT_TERM);
      raftStatus.setLastApplied(RaftUtil.INIT_LOG_INDEX);
      saveLog.writBatch(writeBatch);
      saveData.put(RaftUtil.generateApplyLogKey(groupId),ByteUtil.longToBytes(RaftUtil.INIT_LOG_INDEX));
    } else {
      LogEntries maxLog = saveLog.getMaxLog(RaftUtil.generateLogKey(groupId, Long.MAX_VALUE));
      raftStatus.setCommitIndex(maxLog.getLogIndex());
      raftStatus.setCurrentTerm(maxLog.getTerm());
      byte[] appliedLogIndex = saveData.getValue(RaftUtil.generateApplyLogKey(groupId));
      if (appliedLogIndex == null) {
        //上一次初始化失败时才有可能会走到这里
        if (maxLog.getLogIndex() != RaftUtil.INIT_LOG_INDEX) {
          LOG.error("出现了未知的情况，程序必须退出");
          System.exit(100);
        }
        saveData.put(RaftUtil.generateApplyLogKey(groupId),ByteUtil.longToBytes(RaftUtil.INIT_LOG_INDEX));
        raftStatus.setLastApplied(RaftUtil.INIT_LOG_INDEX);
      }else {
        raftStatus.setLastApplied(
            ByteUtil.bytesToLong(appliedLogIndex));
      }
    }

    return raftStatus;
  }
}
