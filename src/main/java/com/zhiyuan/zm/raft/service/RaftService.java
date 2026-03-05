package com.zhiyuan.zm.raft.service;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.extend.UserWork;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.TaskMaterial;
import com.zhiyuan.zm.raft.exception.RaftFatalException;
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
import com.zhiyuan.zm.raft.util.KeyUtil;

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

  // 组件引用，用于关闭
  private DefaultRpcServer server;
  private ApplyLogTask applyLogTask;
  private SaveLogTask saveLogTask;
  private RoleService roleService;
  private SaveLog saveLog;
  private SaveData saveData;
  private volatile boolean running = false;

  public void start(GlobalConfig conf) throws RocksDBException {
    start(conf, null);
  }

  public void start(GlobalConfig conf, UserWork userWork) throws RocksDBException {
    LOG.info("global conf: " + conf);
    //基础组件
    saveLog = new DefaultSaveLogImpl(conf);
    saveData = new DefaultSaveDataImpl(conf);
    RaftStatus raftStatus = initRaftStatus(saveData, saveLog, conf);
    raftStatus.initDebug();
    BlockingQueue<LogEntries[]> applyLogQueue = new LinkedBlockingDeque<>(1000);
    BlockingQueue<TaskMaterial> saveLogQueue = new LinkedBlockingDeque<>(1000);
    applyLogTask = new ApplyLogTask(applyLogQueue, raftStatus, saveData, saveLog, conf);
    saveLogTask = new SaveLogTask(saveLogQueue, raftStatus, saveLog, conf);

    //核心处理逻辑
    roleService = new RoleService(saveData, conf, raftStatus, new RoleStatus(), saveLog, applyLogQueue,
        saveLogQueue, saveLogTask);
    roleService.setUserWork(userWork);
    RaftRpcHandler raftRpcHandler = new RaftRpcHandler(roleService);
    DataRpcHandler dataRpcHandler = new DataRpcHandler(roleService);

    //网络通信
    server = new DefaultRpcServer(conf, raftRpcHandler, dataRpcHandler);
    server.start();
    applyLogTask.start();
    saveLogTask.start();
    roleService.startWork();
    running = true;
    LOG.info("RaftService started successfully");
  }

  /**
   * 关闭 Raft 服务，优雅地停止所有组件
   */
  public void shutdown() {
    if (!running) {
      LOG.warn("RaftService is not running");
      return;
    }
    LOG.info("Shutting down RaftService");
    running = false;

    // 1. 停止角色服务（这会使角色循环退出）
    if (roleService != null) {
      roleService.shutdown();
    }

    // 2. 停止后台任务
    if (applyLogTask != null) {
      applyLogTask.stop();
    }
    if (saveLogTask != null) {
      saveLogTask.stop();
    }

    // 3. 停止 RPC 服务器
    if (server != null) {
      server.stop();
    }

    // 4. 关闭存储资源
    if (saveLog != null) {
      saveLog.close();
    }
    if (saveData != null) {
      saveData.close();
    }

    LOG.info("RaftService shutdown completed");
  }

  public boolean isRunning() {
    return running;
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

    byte[] bytes = saveLog.getBytes(KeyUtil.generateRaftInitKey(groupId));
    if (bytes == null) {
      saveData.put(KeyUtil.generateApplyLogKey(groupId), ByteUtil.longToBytes(KeyUtil.INIT_LOG_INDEX));
      //这一行应该只有调度节点需要
      saveData.put(KeyUtil.generateCacheTransactionIdKey(), ByteUtil.longToBytes(0L));
     // saveData.put();
      WriteBatch writeBatch = new WriteBatch();
      //写入一个空字节
      writeBatch.put(KeyUtil.generateRaftInitKey(groupId), new byte[] {});
      //初始化一条log
      writeBatch.put(KeyUtil.generateLogKey(groupId, KeyUtil.INIT_LOG_INDEX),
          JSON.toJSONBytes(new LogEntries(KeyUtil.INIT_LOG_INDEX, KeyUtil.INIT_TERM, "")));
      raftStatus.setCurrentTerm(KeyUtil.INIT_TERM);
      raftStatus.setAppliedIndex(KeyUtil.INIT_LOG_INDEX);
      raftStatus.setLastTimeLogIndex(KeyUtil.INIT_LOG_INDEX);
      raftStatus.setLastTimeTerm(KeyUtil.INIT_TERM);
      saveLog.writBatch(writeBatch);
    } else {
      LogEntries maxLog = saveLog.getMaxLog(KeyUtil.generateLogKey(groupId, Long.MAX_VALUE));
      raftStatus.setCurrentTerm(maxLog.getTerm());
      raftStatus.setLastTimeLogIndex(maxLog.getLogIndex());
      raftStatus.setLastTimeTerm(maxLog.getTerm());
      byte[] appliedLogIndex = saveData.getValue(KeyUtil.generateApplyLogKey(groupId));
      if (appliedLogIndex == null) {
        //上一次初始化失败时才有可能会走到这里
        if (maxLog.getLogIndex() != KeyUtil.INIT_LOG_INDEX) {
          throw new RaftFatalException("出现了未知的情况，程序必须退出", 100);
        }
        saveData.put(KeyUtil.generateApplyLogKey(groupId), ByteUtil.longToBytes(KeyUtil.INIT_LOG_INDEX));
        raftStatus.setAppliedIndex(KeyUtil.INIT_LOG_INDEX);
      } else {
        raftStatus.setAppliedIndex(
            ByteUtil.bytesToLong(appliedLogIndex));
      }
    }

    return raftStatus;
  }
}
