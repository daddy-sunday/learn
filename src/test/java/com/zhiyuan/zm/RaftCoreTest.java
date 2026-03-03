package com.zhiyuan.zm;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.constant.ServiceStatus;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.*;
import com.zhiyuan.zm.raft.role.RoleStatus;
import com.zhiyuan.zm.raft.service.RaftStatus;
import org.junit.Assert;
import org.junit.Test;

/**
 * Raft 协议核心 DTO 和状态测试
 * @author zhouzhiyuan
 * @date 2026/03/03
 */
public class RaftCoreTest {

    @Test
    public void testRaftStatus() {
        RaftStatus status = new RaftStatus();

        // 测试基本属性
        Assert.assertEquals(1, status.getGroupId());
        Assert.assertEquals(1, status.getCurrentTerm());
        Assert.assertEquals(0, status.getServiceStatus());
        Assert.assertFalse(status.isInitFlag());

        // 测试设置属性
        status.setGroupId(2);
        status.setCurrentTerm(5L);
        status.setServiceStatus(ServiceStatus.IN_SERVICE);
        status.setInitFlag(true);
        status.setLeaderAddress("localhost:20000");
        status.setLocalAddress("localhost:20001");
        status.setPersonnelNum(5);
        status.setVotedFor("localhost:20000");
        status.setCommitIndex(100L);
        status.setAppliedIndex(90L);
        status.setLastTimeLogIndex(100L);
        status.setLastTimeTerm(5L);

        // 验证设置后的值
        Assert.assertEquals(2, status.getGroupId());
        Assert.assertEquals(5L, status.getCurrentTerm());
        Assert.assertEquals(ServiceStatus.IN_SERVICE, status.getServiceStatus());
        Assert.assertTrue(status.isInitFlag());
        Assert.assertEquals("localhost:20000", status.getLeaderAddress());
        Assert.assertEquals("localhost:20001", status.getLocalAddress());
        Assert.assertEquals(5, status.getPersonnelNum());
        Assert.assertEquals("localhost:20000", status.getVotedFor());
        Assert.assertEquals(100L, status.getCommitIndex());
        Assert.assertEquals(90L, status.getAppliedIndex());
        Assert.assertEquals(100L, status.getLastTimeLogIndex());
        Assert.assertEquals(5L, status.getLastTimeTerm());
    }

    @Test
    public void testRaftStatusMembers() {
        RaftStatus status = new RaftStatus();

        // 测试成员管理
        status.getAllMembers().add("localhost:20000");
        status.getAllMembers().add("localhost:20001");
        status.getAllMembers().add("localhost:20002");

        status.getValidMembers().add("localhost:20000");
        status.getValidMembers().add("localhost:20001");

        Assert.assertEquals(3, status.getAllMembers().size());
        Assert.assertEquals(2, status.getValidMembers().size());
    }

    @Test
    public void testRaftStatusCommitIndex() {
        RaftStatus status = new RaftStatus();

        // 测试 commitIndex 只能增加
        status.setCommitIndex(100L);
        Assert.assertEquals(100L, status.getCommitIndex());

        status.setCommitIndex(50L);  // 应该被忽略
        Assert.assertEquals(100L, status.getCommitIndex());

        status.setCommitIndex(150L);
        Assert.assertEquals(150L, status.getCommitIndex());
    }

    @Test
    public void testCurrentTermAddOne() {
        RaftStatus status = new RaftStatus();
        long initialTerm = status.getCurrentTerm();

        status.currentTermAddOne();
        Assert.assertEquals(initialTerm + 1, status.getCurrentTerm());

        status.currentTermAddOne();
        Assert.assertEquals(initialTerm + 2, status.getCurrentTerm());
    }

    @Test
    public void testGlobalConfig() {
        GlobalConfig config = new GlobalConfig();

        // 测试默认值
        Assert.assertEquals("D:\\tmp\\raft\\log", config.getLogPath());
        Assert.assertEquals("D:\\tmp\\raft\\data", config.getDataPath());
        Assert.assertEquals(20000, config.getCheckTimeoutInterval());
        Assert.assertEquals(10000, config.getSendHeartbeatTimeout());  // 默认 10 秒
        Assert.assertEquals(10000, config.getSendHeartbeatInterval());

        // 测试设置值
        config.setLogPath("/new/log/path");
        config.setDataPath("/new/data/path");
        config.setCheckTimeoutInterval(30000);
        config.setSendHeartbeatTimeout(15000);
        config.setSendHeartbeatInterval(5000);
        config.setOtherNode("localhost:20001,localhost:20002");
        config.setCurrentNode("localhost:20000");
        config.setPort(20000);

        Assert.assertEquals("/new/log/path", config.getLogPath());
        Assert.assertEquals("/new/data/path", config.getDataPath());
        Assert.assertEquals(30000, config.getCheckTimeoutInterval());
        Assert.assertEquals(15000, config.getSendHeartbeatTimeout());
        Assert.assertEquals(5000, config.getSendHeartbeatInterval());
        Assert.assertEquals("localhost:20001,localhost:20002", config.getOtherNode());
        Assert.assertEquals("localhost:20000", config.getCurrentNode());
        Assert.assertEquals(20000, config.getPort());
    }

    @Test
    public void testStatusCode() {
        // 测试状态码常量
        Assert.assertEquals(200, StatusCode.SUCCESS);
        Assert.assertEquals(301, StatusCode.REDIRECT);
        Assert.assertEquals(602, StatusCode.ERROR_REQUEST);
        Assert.assertEquals(500, StatusCode.SYSTEMEXCEPTION);
        Assert.assertEquals(60, StatusCode.SLEEP);
        Assert.assertEquals(601, StatusCode.NON_SEVICE);
    }

    @Test
    public void testServiceStatus() {
        // 测试服务状态常量
        Assert.assertEquals(0, ServiceStatus.NON_SERVICE);
        Assert.assertEquals(1, ServiceStatus.IN_SERVICE);
        Assert.assertEquals(2, ServiceStatus.IN_SWITCH_ROLE);
        Assert.assertEquals(4, ServiceStatus.READ_ONLY);
        Assert.assertEquals(3, ServiceStatus.WAIT_RENEW);
    }

    @Test
    public void testRoleStatus() {
        RoleStatus roleStatus = new RoleStatus();

        // 测试初始状态为 FOLLOWER
        Assert.assertEquals(RoleStatus.FOLLOWER, roleStatus.getNodeStatus());

        // 测试状态转换方法
        roleStatus.followerToCandidate();
        Assert.assertEquals(RoleStatus.CANDIDATE, roleStatus.getNodeStatus());

        roleStatus.candidateToLeader();
        Assert.assertEquals(RoleStatus.LEADER, roleStatus.getNodeStatus());

        roleStatus.leaderToFollower();
        Assert.assertEquals(RoleStatus.FOLLOWER, roleStatus.getNodeStatus());
    }

    @Test
    public void testAddLogRequest() {
        LogEntries entry = new LogEntries(100L, 1L, "test");
        LogEntries[] entries = new LogEntries[]{entry};

        AddLogRequest request = new AddLogRequest(
            100L, 1L, "leader", 99L, 1L, entries, 90L
        );

        Assert.assertEquals(100L, request.getLogIndex());
        Assert.assertEquals(1L, request.getTerm());
        Assert.assertEquals("leader", request.getLeaderId());
        Assert.assertEquals(99L, request.getPrevLogIndex());
        Assert.assertEquals(1L, request.getPreLogTerm());
        Assert.assertEquals(90L, request.getLeaderCommit());
        Assert.assertArrayEquals(entries, request.getEntries());
    }

    @Test
    public void testVoteRequest() {
        VoteRequest request = new VoteRequest();
        request.setTerm(5L);
        request.setCandidateId("localhost:20000");
        request.setLastLogIndex(100L);
        request.setLastLogTerm(5L);

        Assert.assertEquals(5L, request.getTerm());
        Assert.assertEquals("localhost:20000", request.getCandidateId());
        Assert.assertEquals(100L, request.getLastLogIndex());
        Assert.assertEquals(5L, request.getLastLogTerm());
    }

    @Test
    public void testRaftRpcRequest() {
        RaftRpcRequest request = new RaftRpcRequest(1, "test message");

        Assert.assertEquals(1, request.getType());
        Assert.assertEquals("test message", request.getMessage());
    }

    @Test
    public void testRaftRpcResponse() {
        RaftRpcResponest response = new RaftRpcResponest(5L, true);

        Assert.assertEquals(5L, response.getTerm());
        Assert.assertTrue(response.getStatus());
    }

    @Test
    public void testRaftRpcResponseWithFailCause() {
        RaftRpcResponest response = new RaftRpcResponest(5L, false, (byte) 1);

        Assert.assertEquals(5L, response.getTerm());
        Assert.assertFalse(response.getStatus());
        Assert.assertEquals((byte) 1, response.getFailCause());
    }
}
