package com.zhiyuan.zm.raft.monitor.handler;

import com.alibaba.fastjson.JSON;
import com.zhiyuan.zm.raft.dto.monitor.MetricsDTO;
import com.zhiyuan.zm.raft.dto.monitor.MonitorResponseDTO;
import com.zhiyuan.zm.raft.dto.monitor.NodeStatusDTO;
import com.zhiyuan.zm.raft.dto.monitor.TransactionStatusDTO;
import com.zhiyuan.zm.raft.monitor.MonitorService;

import java.util.Map;

/**
 * API 请求处理器
 * 处理所有监控相关的 API 请求
 *
 * @author zhouzhiyuan
 * @date 2026/03/06
 */
public class ApiHandler {

    private final MonitorService monitorService;

    public ApiHandler(MonitorService monitorService) {
        this.monitorService = monitorService;
    }

    /**
     * 处理集群状态请求
     * GET /api/cluster
     */
    public String handleClusterStatus() {
        try {
            Map<String, Object> status = monitorService.getClusterStatus();
            return JSON.toJSONString(MonitorResponseDTO.success(status));
        } catch (Exception e) {
            return JSON.toJSONString(MonitorResponseDTO.error(500, "Failed to get cluster status: " + e.getMessage()));
        }
    }

    /**
     * 处理节点状态请求
     * GET /api/node/status
     */
    public String handleNodeStatus() {
        try {
            NodeStatusDTO nodeStatus = monitorService.getNodeStatus();
            return JSON.toJSONString(MonitorResponseDTO.success(nodeStatus));
        } catch (Exception e) {
            return JSON.toJSONString(MonitorResponseDTO.error(500, "Failed to get node status: " + e.getMessage()));
        }
    }

    /**
     * 处理节点指标请求
     * GET /api/node/metrics
     */
    public String handleNodeMetrics() {
        try {
            MetricsDTO metrics = monitorService.getMetrics();
            return JSON.toJSONString(MonitorResponseDTO.success(metrics));
        } catch (Exception e) {
            return JSON.toJSONString(MonitorResponseDTO.error(500, "Failed to get metrics: " + e.getMessage()));
        }
    }

    /**
     * 处理事务状态请求
     * GET /api/transactions
     */
    public String handleTransactionStatus() {
        try {
            TransactionStatusDTO transactionStatus = monitorService.getTransactionStatus();
            return JSON.toJSONString(MonitorResponseDTO.success(transactionStatus));
        } catch (Exception e) {
            return JSON.toJSONString(MonitorResponseDTO.error(500, "Failed to get transaction status: " + e.getMessage()));
        }
    }

    /**
     * 处理存储统计请求
     * GET /api/storage
     */
    public String handleStorageStats() {
        try {
            Map<String, Object> stats = monitorService.getStorageStats();
            return JSON.toJSONString(MonitorResponseDTO.success(stats));
        } catch (Exception e) {
            return JSON.toJSONString(MonitorResponseDTO.error(500, "Failed to get storage stats: " + e.getMessage()));
        }
    }
}
