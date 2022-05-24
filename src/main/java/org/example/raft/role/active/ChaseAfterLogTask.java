package org.example.raft.role.active;

import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.example.raft.constant.MessageType;
import org.example.raft.constant.StatusCode;
import org.example.raft.dto.ChaseAfterLog;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.SynchronizeLogResult;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.RoleStatus;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.RaftUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 *@author zhouzhiyuan
 *@date 2021/11/26
 */
public class ChaseAfterLogTask {

  private static final Logger LOG = LoggerFactory.getLogger(ChaseAfterLogTask.class);

  private RaftStatus status;

  private RoleStatus roleStatus;

  private SaveLog saveLog;

  private ScheduledExecutorService executorService;

  private volatile byte serviceStatus = 0;

  private volatile long serviceCount = 0;

  public ChaseAfterLogTask(RaftStatus status, RoleStatus roleStatus, SaveLog saveLog) {
    this.status = status;
    this.roleStatus = roleStatus;
    this.saveLog = saveLog;
  }

  public void start() {
    executorService = new ScheduledThreadPoolExecutor(1, e -> new Thread(e, "ChaseAfterLogTask"));
    executorService.scheduleAtFixedRate(this::run, 0, 50, TimeUnit.MILLISECONDS);
  }


  /**
   * 单线程追加进度落后的节点log，成功后将节点添加到可用列表
   * todo 追赶前需要获取一下目标节点的 log 状态，预先判断一下发送的log是否符合连续自增的条件，如果不符合发送了也是失败
   */
  public void run() {
    serviceStatus = 1;
    serviceCount += 1;
    Iterator<ChaseAfterLog> iterator = status.getFailedMembers().iterator();
    while (iterator.hasNext()) {
      ChaseAfterLog failedMember = iterator.next();
      String address = failedMember.getAddress();
      long logId = failedMember.getLogId();
      RaftRpcRequest raftRpcRequest = new RaftRpcRequest(MessageType.LOG, null);
      SendHeartbeat sendHeartbeat = new SendHeartbeat(roleStatus, raftRpcRequest, address, status.getCurrentTerm());

      while (roleStatus.getNodeStatus() == RoleStatus.LEADER) {
        if (status.getCommitIndex() <= logId) {
          LogEntries log;
          try {
            //todo 优化 不能每次都访问存储
            log = saveLog.get(RaftUtil.generateLogKey(status.getGroupId(), logId));
          } catch (RocksDBException e) {
            LOG.error(e.getMessage(), e);
            return;
          }
          if (log != null) {
            raftRpcRequest.setMessage(JSON.toJSONString(log));
            Future<SynchronizeLogResult> submit = executorService.submit(sendHeartbeat);
            try {
              if (submit.get().isSuccess()) {
                logId++;
              } else {
                if (submit.get().getStatusCode() == StatusCode.EXCEPTION) {
                  LOG.error("leader-> chase after log  failed address: " + address);
                  failedMember.setLogId(logId);
                  break;
                }
                if (submit.get().getStatusCode() == StatusCode.NOT_MATCH_LOG_INDEX) {
                  logId--;
                }
              }
            } catch (Exception e) {
              LOG.error("leader-> chase after log: " + e.getLocalizedMessage(), e);
              break;
            }
          } else {
            LOG.error("leader-> get history log  failed   ");
            break;
          }
        } else {
          //追加完成
          iterator.remove();
          status.getValidMembers().add(address);
          break;
        }
      }
    }
    serviceStatus = 0;
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
