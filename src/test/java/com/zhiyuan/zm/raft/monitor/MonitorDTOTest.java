package com.zhiyuan.zm.raft.monitor;

import static org.junit.Assert.*;

import org.junit.Test;

import com.zhiyuan.zm.raft.dto.monitor.ClusterStatusDTO;
import com.zhiyuan.zm.raft.dto.monitor.MetricsDTO;
import com.zhiyuan.zm.raft.dto.monitor.MonitorResponseDTO;
import com.zhiyuan.zm.raft.dto.monitor.NodeStatusDTO;
import com.zhiyuan.zm.raft.dto.monitor.TransactionStatusDTO;

/**
 * 监控 DTO 测试
 * @author zhouzhiyuan
 * @date 2026/03/06
 */
public class MonitorDTOTest {

    @Test
    public void testClusterStatusDTO() {
        ClusterStatusDTO dto = new ClusterStatusDTO();
        dto.setHealthy(true);
        dto.setLeaderAddress("localhost:20000");
        dto.setCurrentTerm(1);
        dto.setTotalMembers(3);
        dto.setValidMembers(3);
        dto.setFailedMembers(0);
        dto.setStatus("HEALTHY");

        assertTrue(dto.isHealthy());
        assertEquals("localhost:20000", dto.getLeaderAddress());
        assertEquals(1, dto.getCurrentTerm());
        assertEquals(3, dto.getTotalMembers());
        assertEquals(3, dto.getValidMembers());
        assertEquals(0, dto.getFailedMembers());
        assertEquals("HEALTHY", dto.getStatus());
    }

    @Test
    public void testNodeStatusDTO() {
        NodeStatusDTO dto = new NodeStatusDTO();
        dto.setAddress("localhost:20000");
        dto.setRole("LEADER");
        dto.setServiceStatus("IN_SERVICE");
        dto.setCurrentTerm(1);
        dto.setCommitIndex(100);
        dto.setAppliedIndex(99);
        dto.setMaxLogIndex(100);
        dto.setLocal(true);

        assertEquals("localhost:20000", dto.getAddress());
        assertEquals("LEADER", dto.getRole());
        assertEquals("IN_SERVICE", dto.getServiceStatus());
        assertEquals(1, dto.getCurrentTerm());
        assertEquals(100, dto.getCommitIndex());
        assertEquals(99, dto.getAppliedIndex());
        assertEquals(100, dto.getMaxLogIndex());
        assertTrue(dto.isLocal());
    }

    @Test
    public void testMetricsDTO() {
        MetricsDTO dto = new MetricsDTO();
        dto.setAddress("localhost:20000");
        dto.setSynLogQueueSize(5);
        dto.setApplyLogQueueSize(3);
        dto.setSaveLogQueueSize(2);
        dto.setBusynessStatus(100);
        dto.setLeaseEndTime(System.currentTimeMillis() + 10000);
        dto.setLogIndex(100);

        assertEquals("localhost:20000", dto.getAddress());
        assertEquals(5, dto.getSynLogQueueSize());
        assertEquals(3, dto.getApplyLogQueueSize());
        assertEquals(2, dto.getSaveLogQueueSize());
        assertEquals(100, dto.getBusynessStatus());
        assertEquals(100, dto.getLogIndex());
    }

    @Test
    public void testTransactionStatusDTO() {
        TransactionStatusDTO dto = new TransactionStatusDTO();
        dto.setActiveTransactionCount(5);
        dto.setAllocatedTransactionId(100);
        dto.setMaxTransactionId(150);

        TransactionStatusDTO.TransactionDistributionDTO dist =
            new TransactionStatusDTO.TransactionDistributionDTO();
        dist.setOpenCount(3);
        dist.setCloseCount(1);
        dist.setRollbackCount(1);
        dto.setDistribution(dist);

        assertEquals(5, dto.getActiveTransactionCount());
        assertEquals(100, dto.getAllocatedTransactionId());
        assertEquals(150, dto.getMaxTransactionId());
        assertEquals(3, dto.getDistribution().getOpenCount());
        assertEquals(1, dto.getDistribution().getCloseCount());
        assertEquals(1, dto.getDistribution().getRollbackCount());
    }

    @Test
    public void testMonitorResponseDTO() {
        MonitorResponseDTO successResp = MonitorResponseDTO.success("test data");
        assertEquals(200, successResp.getCode());
        assertEquals("success", successResp.getMessage());
        assertEquals("test data", successResp.getData());

        MonitorResponseDTO errorResp = MonitorResponseDTO.error(500, "error message");
        assertEquals(500, errorResp.getCode());
        assertEquals("error message", errorResp.getMessage());
        assertNull(errorResp.getData());
    }
}
