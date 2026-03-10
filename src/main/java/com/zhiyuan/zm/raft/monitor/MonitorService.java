package com.zhiyuan.zm.raft.monitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import com.zhiyuan.zm.raft.dto.ChaseAfterLog;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.TaskMaterial;
import com.zhiyuan.zm.raft.dto.monitor.MetricsDTO;
import com.zhiyuan.zm.raft.dto.monitor.MetricsDTO.FailedMemberDTO;
import com.zhiyuan.zm.raft.dto.monitor.NodeStatusDTO;
import com.zhiyuan.zm.raft.dto.monitor.TransactionStatusDTO;
import com.zhiyuan.zm.raft.dto.monitor.TransactionStatusDTO.ActiveTransactionDTO;
import com.zhiyuan.zm.raft.dto.monitor.TransactionStatusDTO.TransactionDistributionDTO;
import com.zhiyuan.zm.raft.persistence.SaveLog;
import com.zhiyuan.zm.raft.role.LeaderRole;
import com.zhiyuan.zm.raft.role.RoleStatus;
import com.zhiyuan.zm.raft.role.transaction.TransactionService;
import com.zhiyuan.zm.raft.role.transaction.TransactionService.TransactionInfo;
import com.zhiyuan.zm.raft.service.RaftStatus;
import com.zhiyuan.zm.raft.service.RoleService;
import com.zhiyuan.zm.raft.util.KeyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 监控数据收集服务
 * 负责从 Raft 各组件收集监控数据
 *
 * @author zhouzhiyuan
 * @date 2026/03/06
 */
public class MonitorService {

    private static final Logger LOG = LoggerFactory.getLogger(MonitorService.class);

    private final RaftStatus raftStatus;
    private final RoleService roleService;
    private final SaveLog saveLog;
    private final BlockingQueue<TaskMaterial> saveLogQueue;
    private final BlockingQueue<LogEntries[]> applyLogQueue;
    private final BlockingQueue<TaskMaterial> synLogQueue;

    // 用于访问 LeaderRole 的内部状态
    private volatile LeaderRole leaderRole;

    public MonitorService(RaftStatus raftStatus, RoleService roleService, SaveLog saveLog,
                         BlockingQueue<TaskMaterial> saveLogQueue, BlockingQueue<LogEntries[]> applyLogQueue,
                         BlockingQueue<TaskMaterial> synLogQueue) {
        this.raftStatus = raftStatus;
        this.roleService = roleService;
        this.saveLog = saveLog;
        this.saveLogQueue = saveLogQueue;
        this.applyLogQueue = applyLogQueue;
        this.synLogQueue = synLogQueue;
    }

    /**
     * 设置 LeaderRole 引用（用于获取 Leader 特有指标）
     */
    public void setLeaderRole(LeaderRole leaderRole) {
        this.leaderRole = leaderRole;
    }

    /**
     * 获取集群状态
     */
    public Map<String, Object> getClusterStatus() {
        Map<String, Object> status = new HashMap<>();

        // 集群健康状态
        boolean healthy = raftStatus.getValidMembers().size() >= (raftStatus.getPersonnelNum() / 2 + 1);
        status.put("healthy", healthy);

        // Leader 地址
        status.put("leaderAddress", raftStatus.getLeaderAddress());

        // 当前 Term
        status.put("currentTerm", raftStatus.getCurrentTerm());

        // 成员统计
        status.put("totalMembers", raftStatus.getPersonnelNum());
        status.put("validMembers", raftStatus.getValidMembers().size());
        status.put("failedMembers", raftStatus.getFailedMembers().size());

        // 成员列表
        List<String> members = new ArrayList<>();
        members.add(raftStatus.getLocalAddress());
        members.addAll(raftStatus.getAllMembers());
        status.put("members", members);

        // 集群状态描述
        String statusDesc;
        if (healthy) {
            statusDesc = "HEALTHY";
        } else {
            statusDesc = "UNHEALTHY - Not enough valid members";
        }
        status.put("status", statusDesc);

        return status;
    }

    /**
     * 获取当前节点状态
     */
    public NodeStatusDTO getNodeStatus() {
        int nodeRole = roleService.getCurrentRoleStatus();
        String roleStr;
        switch (nodeRole) {
            case RoleStatus.LEADER:
                roleStr = "LEADER";
                break;
            case RoleStatus.FOLLOWER:
                roleStr = "FOLLOWER";
                break;
            case RoleStatus.CANDIDATE:
                roleStr = "CANDIDATE";
                break;
            case RoleStatus.LEARNER:
                roleStr = "LEARNER";
                break;
            default:
                roleStr = "UNKNOWN";
        }

        // 获取 maxLogIndex
        long maxLogIndex = getMaxLogIndex();

        return new NodeStatusDTO(
            raftStatus.getLocalAddress(),
            roleStr,
            getServiceStatusString(raftStatus.getServiceStatus()),
            raftStatus.getCurrentTerm(),
            raftStatus.getCommitIndex(),
            raftStatus.getAppliedIndex(),
            maxLogIndex,
            raftStatus.getLastUpdateTime(),
            true
        );
    }

    /**
     * 获取节点详细指标
     */
    public MetricsDTO getMetrics() {
        // 队列大小
        int synLogQueueSize = synLogQueue != null ? synLogQueue.size() : 0;
        int applyLogQueueSize = applyLogQueue != null ? applyLogQueue.size() : 0;
        int saveLogQueueSize = saveLogQueue != null ? saveLogQueue.size() : 0;

        // 有效成员
        List<String> validMembers = new ArrayList<>(raftStatus.getValidMembers());

        // 失败成员
        List<FailedMemberDTO> failedMembers = new ArrayList<>();
        for (ChaseAfterLog log : raftStatus.getFailedMembers()) {
            failedMembers.add(new FailedMemberDTO(
                log.getAddress(),
                log.getRaftGroupId(),
                log.getLogId()
            ));
        }

        // Leader 特有指标
        long busynessStatus = 0;
        long leaseEndTime = 0;
        long logIndex = 0;

        if (leaderRole != null) {
            busynessStatus = leaderRole.busynessStatus;
            leaseEndTime = leaderRole.leaseEndTime;
            logIndex = leaderRole.getLogIndex();
        }

        return new MetricsDTO(
            raftStatus.getLocalAddress(),
            synLogQueueSize,
            applyLogQueueSize,
            saveLogQueueSize,
            validMembers,
            failedMembers,
            busynessStatus,
            leaseEndTime,
            logIndex
        );
    }

    /**
     * 获取事务状态
     */
    public TransactionStatusDTO getTransactionStatus() {
        TransactionStatusDTO dto = new TransactionStatusDTO();

        // 设置默认值
        dto.setActiveTransactionCount(0);
        dto.setAllocatedTransactionId(0);
        dto.setMaxTransactionId(0);
        dto.setDistribution(new TransactionDistributionDTO(0, 0, 0));
        dto.setActiveTransactions(new ArrayList<>());

        // 从 LeaderRole 获取事务服务
        if (leaderRole != null) {
            try {
                TransactionService transactionService = leaderRole.getTransactionService();
                if (transactionService != null) {
                    // 获取活跃事务数
                    int activeCount = transactionService.getActiveTransactionCount();
                    dto.setActiveTransactionCount(activeCount);

                    // 获取已分配的事务 ID
                    dto.setAllocatedTransactionId(transactionService.getAllocatedTransactionId());
                    dto.setMaxTransactionId(transactionService.getMaxTransactionId());

                    // 获取事务状态分布
                    Map<String, Integer> distribution = transactionService.getTransactionDistribution();
                    TransactionDistributionDTO distDTO = new TransactionDistributionDTO(
                        distribution.getOrDefault("OPEN", 0),
                        distribution.getOrDefault("CLOSE", 0),
                        distribution.getOrDefault("ROLLBACK", 0)
                    );
                    dto.setDistribution(distDTO);

                    // 获取活跃事务列表
                    List<ActiveTransactionDTO> activeTransactions = new ArrayList<>();
                    Map<String, TransactionInfo> activeTxMap = transactionService.getActiveTransactions();
                    for (Map.Entry<String, TransactionInfo> entry : activeTxMap.entrySet()) {
                        TransactionInfo info = entry.getValue();
                        activeTransactions.add(new ActiveTransactionDTO(
                            entry.getKey(),
                            info.getTransactionId() != null ? info.getTransactionId() : 0,
                            info.getStatus() != null ? info.getStatus().name() : "UNKNOWN",
                            info.getTimestamp(),
                            info.getDataCount(),
                            info.getSnapshotTs(),
                            info.getBeginTs(),
                            info.getWriteSet()
                        ));
                    }
                    dto.setActiveTransactions(activeTransactions);
                }
            } catch (Exception e) {
                LOG.warn("获取事务状态失败", e);
            }
        }

        return dto;
    }

    /**
     * 获取存储统计信息
     */
    public Map<String, Object> getStorageStats() {
        Map<String, Object> stats = new HashMap<>();

        // 估算日志数量（通过 maxLogIndex）
        long maxLogIndex = getMaxLogIndex();
        stats.put("logEntryCount", maxLogIndex - KeyUtil.INIT_LOG_INDEX);

        // RocksDB 相关信息（暂不可用，需要 SaveData 提供接口）
        stats.put("dataSize", "N/A");

        return stats;
    }

    /**
     * 获取最大日志索引
     */
    private long getMaxLogIndex() {
        try {
            byte[] maxKey = KeyUtil.generateLogKey(raftStatus.getGroupId(), Long.MAX_VALUE);
            LogEntries maxLog = saveLog.getMaxLog(maxKey);
            return maxLog != null ? maxLog.getLogIndex() : KeyUtil.INIT_LOG_INDEX;
        } catch (Exception e) {
            LOG.warn("获取最大日志索引失败：{}", e.getMessage());
            return KeyUtil.INIT_LOG_INDEX;
        }
    }

    /**
     * 将 serviceStatus 字节转换为字符串
     */
    private String getServiceStatusString(byte status) {
        switch (status) {
            case 0: return "OFFLINE";
            case 1: return "IN_SERVICE";
            case 2: return "NON_SERVICE";
            case 3: return "READ_ONLY";
            case 4: return "IN_SWITCH_ROLE";
            case 5: return "WAIT_RENEW";
            default: return "UNKNOWN(" + status + ")";
        }
    }

    /**
     * 获取当前角色状态
     */
    public int getCurrentRoleStatus() {
        return roleService.getCurrentRoleStatus();
    }
}
