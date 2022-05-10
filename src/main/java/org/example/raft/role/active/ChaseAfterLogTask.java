package org.example.raft.role.active;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import org.example.raft.constant.MessageType;
import org.example.raft.constant.StatusCode;
import org.example.raft.dto.AddLog;
import org.example.raft.dto.ChaseAfterLog;
import org.example.raft.dto.RaftRpcRequest;
import org.example.raft.dto.SynchronizeLogResult;
import org.example.raft.persistence.SaveLog;
import org.example.raft.role.RoleStatus;
import org.example.raft.service.RaftStatus;
import org.example.raft.util.ByteUtil;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 *@author zhouzhiyuan
 *@date 2021/11/26
 */
public class ChaseAfterLogTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(ChaseAfterLogTask.class);

  private RaftStatus status;

  private RoleStatus roleStatus;

  private SaveLog saveLog;

  private ExecutorService executorService;

  public ChaseAfterLogTask(RaftStatus status, RoleStatus roleStatus, SaveLog saveLog,
      ExecutorService executorService) {
    this.status = status;
    this.roleStatus = roleStatus;
    this.saveLog = saveLog;
    this.executorService = executorService;
  }

  /**
   * 单线程追加进度落后的节点log，成功后将节点添加到可用列表
   * todo 追赶前需要获取一下目标节点的 log 状态，预先判断一下发送的log是否符合连续自增的条件，如果不符合发送了也是失败
   */
  @Override
  public void run() {
    for (ChaseAfterLog failedMember : status.getFailedMembers()) {
      String address = failedMember.getAddress();
      long logId = failedMember.getLogId();
      RaftRpcRequest raftRpcRequest = new RaftRpcRequest(MessageType.LOG, null);
      SendHeartbeat sendHeartbeat = new SendHeartbeat(roleStatus,raftRpcRequest, address, status.getCurrentTerm());

      while (roleStatus.getNodeStatus() == RoleStatus.LEADER ) {
        if (status.getCommitIndex() <= logId) {
          AddLog log = null;
          try {
            log = saveLog.get(ByteUtil.concatLogId(status.getGroupId(), logId));
          } catch (RocksDBException e) {
            LOG.error(e.getMessage(),e);
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
                  LOG.error("leader-> chase after log  failed address: "+address);
                  failedMember.setLogId(logId);
                  break;
                }
                logId--;
              }
            } catch (Exception e) {
              LOG.error("leader-> chase after log: "+e.getLocalizedMessage(),e);
              break;
            }
          }{
            LOG.error("leader-> get history log  failed   ");
            break;
          }
        }else {
          //追加完成
          status.getValidMembers().add(address);
          break;
        }
      }
    }
  }
}
