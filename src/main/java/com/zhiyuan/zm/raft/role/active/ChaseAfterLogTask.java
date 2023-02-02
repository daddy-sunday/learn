package com.zhiyuan.zm.raft.role.active;

import java.util.Iterator;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.zhiyuan.zm.raft.constant.MessageType;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.AddLogRequest;
import com.zhiyuan.zm.raft.dto.ChaseAfterLog;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.RaftRpcRequest;
import com.zhiyuan.zm.raft.dto.SynchronizeLogResult;
import com.zhiyuan.zm.raft.persistence.SaveLog;
import com.zhiyuan.zm.raft.role.RoleStatus;
import com.zhiyuan.zm.raft.service.RaftStatus;
import com.zhiyuan.zm.raft.util.RaftUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * @author zhouzhiyuan
 * @date 2021/11/26
 */
public class ChaseAfterLogTask {

  private static final Logger LOG = LoggerFactory.getLogger(ChaseAfterLogTask.class);

  private RaftStatus status;

  private RoleStatus roleStatus;

  private SaveLog saveLog;

  private ScheduledExecutorService executorService;

  private volatile byte serviceStatus = 0;

  private volatile long serviceCount = 0;

  private int sendHeartbeatTimeout;

  public ChaseAfterLogTask(RaftStatus status, RoleStatus roleStatus, SaveLog saveLog, int sendHeartbeatTimeout) {
    this.sendHeartbeatTimeout = sendHeartbeatTimeout;
    this.status = status;
    this.roleStatus = roleStatus;
    this.saveLog = saveLog;
  }

  public void start(long interval) {
    executorService = new ScheduledThreadPoolExecutor(1,new ThreadFactoryBuilder()
        .setDaemon(true).setNameFormat("ChaseAfterLogTask").build());
    executorService.scheduleAtFixedRate(this::run, 0, interval, TimeUnit.MILLISECONDS);
  }


  /**
   * 单线程追加进度落后的节点log，成功后将节点添加到可用列表
   * todo 追赶前需要获取一下目标节点的 logIndex ，预先判断一下发送的log是否符合连续自增的条件，如果不符合发送了也是失败
   *  当发现日志差很多时，需要使用snapshot 发送快照的方式进行数据同步。当支持了这种方式时就可以删除陈旧的log了，只保留一定量或者一段时间内的log。减少存储压力。
   */
  public void run() {
    try {

      serviceStatus = 1;
      serviceCount += 1;
      Iterator<ChaseAfterLog> iterator = status.getFailedMembers().iterator();
      while (iterator.hasNext()) {
        ChaseAfterLog failedMember = iterator.next();
        String address = failedMember.getAddress();
        long logId = failedMember.getLogId();
        RaftRpcRequest raftRpcRequest = new RaftRpcRequest(MessageType.LOG, null);
        SendHeartbeat sendHeartbeat = new SendHeartbeat(roleStatus, raftRpcRequest, address, sendHeartbeatTimeout);

        while (roleStatus.getNodeStatus() == RoleStatus.LEADER) {
          LOG.info("开始追log日志 地址：" + address + " logIndex : " + logId);
          LogEntries log;
          LogEntries prevLog;
          try {
            //todo 优化 ：当第一次追日志成功后，开始批量发送日志，一条一条发送太慢了
            log = saveLog.get(RaftUtil.generateLogKey(status.getGroupId(), logId));
            prevLog = saveLog.get(RaftUtil.generateLogKey(status.getGroupId(), logId - 1));
          } catch (RocksDBException e) {
            LOG.error(e.getMessage(), e);
            System.exit(100);
            return;
          }

          if (log != null) {
            AddLogRequest addLogRequest = new AddLogRequest(logId, status.getCurrentTerm(), status.getLocalAddress(),
                logId - 1, prevLog.getTerm(), new LogEntries[] {log}, status.getCommitIndex());

            raftRpcRequest.setMessage(JSON.toJSONString(addLogRequest));
            SynchronizeLogResult result = sendHeartbeat.call();
            try {
              if (result.isSuccess()) {
                logId++;
              } else {
                if (result.getStatusCode() == StatusCode.EXCEPTION) {
                  LOG.error("追log异常 地址: " + address + " raftGroupId：" + status.getGroupId());
                  failedMember.setLogId(logId);
                  break;
                }
                if (result.getStatusCode() == StatusCode.NOT_MATCH_LOG_INDEX) {
                  logId--;
                  continue;
                }
                LOG.error("追任务失败 ：" + address + " 组id：" + status.getGroupId() + " " + result);
                break;
              }
            } catch (Exception e) {
              LOG.error("leader-> chase after log: " + e.getLocalizedMessage(), e);
              break;
            }
          } else {
            //追加完成
            LOG.info("追log完成 地址为：" + address + " 组id：" + status.getGroupId() + "logIndex: " + logId);
            iterator.remove();
            status.getValidMembers().add(address);
            break;
          }
        }
      }
      serviceStatus = 0;
    } catch (Exception e) {
      LOG.error("追赶日志线程异常: "+e.getMessage(), e);
    }
  }


  public void stop() {
    executorService.shutdownNow();
  }


  public byte getServiceStatus() {
    return serviceStatus;
  }

  public void setServiceStatus(byte serviceStatus) {
    this.serviceStatus = serviceStatus;
  }
}
