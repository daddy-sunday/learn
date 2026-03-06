1# Raft 项目 Web UI 监控功能

## 功能概述

本项目实现了一个基于嵌入式 HTTP 服务器的 Web UI 监控功能，用于实时监控 Raft 集群的运行状态。

## 技术栈

- **后端**: NanoHTTPD (轻量级 HTTP 服务器)
- **前端**: Vue.js 2.6 + Bootstrap 4.6
- **通信**: RESTful API + 定时轮询 (5 秒间隔)

## 快速开始

### 1. 启动 Raft 节点

运行 `RaftServiceTest` 中的任意测试方法启动 Raft 节点：

```java
// src/test/java/com/zhiyuan/zm/RaftServiceTest.java
@Test
public void startNode1() throws Exception {
    // 启动节点 1
}
```

### 2. 访问监控页面

启动节点后，打开浏览器访问：

```
http://localhost:8080/
```

> **注意**：监控页面根路径 `/` 和 `/monitor` 都可以访问

## 监控指标

### 集群级别监控
- 集群整体健康状态
- 当前 Leader 节点地址
- 集群成员列表及状态
- 当前 Term（任期）
- 有效成员数（显示格式：总节点数 - 失败节点数/总节点数）

### 节点级别监控
- 节点地址和角色（Leader/Follower/Candidate/Learner）
- 服务状态（IN_SERVICE/NON_SERVICE/READ_ONLY 等）
- currentTerm（当前任期）
- commitIndex（已提交索引）
- appliedIndex（已应用索引）
- maxLogIndex（最大日志索引）

### 日志复制监控
- 有效成员队列（validMembers）
- 失败成员队列（failedMembers）及追赶状态
- 同步日志队列大小（synLogQueue）
- 应用日志队列大小（applyLogQueue）
- 保存日志队列大小（saveLogQueue）

### 事务监控
- 当前活跃事务数
- 事务 ID 分配情况
- 事务状态分布（OPEN/CLOSE/ROLLBACK）
- MVCC 事务详细信息

## API 接口

| 接口 | 方法 | 描述 |
|------|------|------|
| `/api/cluster` | GET | 获取集群整体状态 |
| `/api/node/status` | GET | 获取当前节点状态 |
| `/api/node/metrics` | GET | 获取节点详细指标 |
| `/api/transactions` | GET | 获取事务状态 |
| `/api/storage` | GET | 获取存储统计信息 |
| `/api/health` | GET | 健康检查 |

### API 响应格式

所有 API 返回统一的 JSON 格式：

```json
{
  "code": 200,
  "message": "success",
  "data": { ... }
}
```

### 示例

#### 获取集群状态

```bash
curl http://localhost:8080/api/cluster
```

响应：
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "healthy": true,
    "leaderAddress": "localhost:20000",
    "currentTerm": 1,
    "totalMembers": 3,
    "validMembers": 3,
    "failedMembers": 0,
    "status": "HEALTHY"
  }
}
```

#### 获取节点状态

```bash
curl http://localhost:8080/api/node/status
```

#### 获取事务状态

```bash
curl http://localhost:8080/api/transactions
```

响应：
```json
{
  "code": 200,
  "message": "success",
  "data": {
    "activeTransactionCount": 0,
    "activeTransactions": [],
    "allocatedTransactionId": 0,
    "distribution": {
      "closeCount": 0,
      "openCount": 0,
      "rollbackCount": 0
    },
    "maxTransactionId": 0
  }
}
```

## 配置说明

### 修改监控端口和启用状态

在启动 Raft 节点前，通过 `GlobalConfig` 设置监控配置：

```java
GlobalConfig conf = new GlobalConfig();
conf.setMonitorEnabled(true);  // 启用/禁用监控（默认启用）
conf.setMonitorPort(8080);     // 设置监控端口（默认 8080）
```

### 配置项说明

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `monitorEnabled` | boolean | `true` | 是否启用 Web UI 监控服务 |
| `monitorPort` | int | `8080` | 监控服务 HTTP 端口 |

### 禁用监控服务

```java
GlobalConfig conf = new GlobalConfig();
conf.setMonitorEnabled(false); // 禁用监控服务
```

### 修改监控端口

```java
GlobalConfig conf = new GlobalConfig();
conf.setMonitorPort(9090); // 修改为 9090 端口
```

### 多节点测试配置

在测试环境中启动多个节点时，需要为每个节点设置不同的监控端口：

```java
// 节点 1
GlobalConfig conf1 = new GlobalConfig();
conf1.setPort(20000);
conf1.setMonitorPort(8080);  // 节点 1 监控端口

// 节点 2
GlobalConfig conf2 = new GlobalConfig();
conf2.setPort(20001);
conf2.setMonitorPort(8081);  // 节点 2 监控端口

// 节点 3
GlobalConfig conf3 = new GlobalConfig();
conf3.setPort(20002);
conf3.setMonitorPort(8082);  // 节点 3 监控端口
```

测试代码示例（`RaftServiceTest.java`）：
```java
@Test
public void server1() throws Exception {
    GlobalConfig globalConfig = new GlobalConfig();
    globalConfig.setPort(20000);
    globalConfig.setMonitorPort(8080);  // 设置监控端口
    RaftService raftService = new RaftService();
    raftService.start(globalConfig);
}
```

## 实时刷新

前端页面默认每 5 秒自动刷新一次数据。也可以点击页面右上角的"刷新"按钮手动刷新。

## 项目结构

```
src/main/java/com/zhiyuan/zm/raft/monitor/
├── MonitorService.java          # 监控数据收集服务
├── MonitorServer.java           # HTTP 服务器
├── handler/
│   └── ApiHandler.java          # API 请求处理器
└── ...

src/main/resources/web/
└── index.html                   # 前端页面

src/test/java/com/zhiyuan/zm/raft/monitor/
└── MonitorDTOTest.java          # 单元测试
```

## 监控页面截图功能

- **集群状态卡片**: 显示集群健康状态、Leader 地址、Term、成员统计
- **节点状态卡片**: 显示当前节点角色、服务状态、日志索引等
- **队列状态卡片**: 显示各种队列大小、有效成员、失败成员
- **事务状态卡片**: 显示活跃事务数、事务状态分布、活跃事务列表
- **最近日志**: 显示最近的 Raft 日志条目

## 注意事项

1. **端口占用**: 确保监控端口（默认 8080）未被占用
2. **网络访问**: 监控服务仅绑定本地地址，外部访问需要配置
3. **性能影响**: 监控服务会占用少量系统资源，生产环境建议评估影响
4. **安全性**: 当前实现无认证授权，内网使用建议添加访问控制
5. **CDN 依赖**: 前端使用 CDN 加载 Vue.js、Bootstrap 和 Font Awesome，需要网络连接

## 已修复的问题

### 1. Font Awesome 图标无法加载（2026-03-06）

**问题**: CDN 地址错误导致图标无法加载

```html
<!-- 错误 -->
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@fortawesome/font-awesome@4.7.0/css/font-awesome.min.css">

<!-- 正确 -->
<link rel="stylesheet" href="https://cdn.staticfile.org/font-awesome/4.7.0/css/font-awesome.min.css">
```

### 2. 事务状态 distribution 字段为空（2026-03-06）

**问题**: 当节点不是 Leader 时，`distribution` 字段为 `null`

**修复**: 在 `MonitorService.getTransactionStatus()` 中设置默认值

```java
// 设置默认值
dto.setActiveTransactionCount(0);
dto.setAllocatedTransactionId(0);
dto.setMaxTransactionId(0);
dto.setDistribution(new TransactionDistributionDTO(0, 0, 0));
dto.setActiveTransactions(new ArrayList<>());
```

### 3. CDN 资源无法加载导致页面显示模板变量（2026-03-06）

**问题**: 页面显示 `{{ nodeStatus.address }}` 等模板变量原文，而不是实际数据

**原因**: jsdelivr CDN 在国内访问不稳定，Vue.js 和 Axios 无法加载

**修复**: 更换为国内可访问的 staticfile CDN

```html
<!-- 修复前（jsdelivr，可能无法访问） -->
<script src="https://cdn.jsdelivr.net/npm/vue@2.6.14/dist/vue.js"></script>
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/bootstrap@4.6.0/dist/css/bootstrap.min.css">

<!-- 修复后（staticfile，国内可访问） -->
<script src="https://cdn.staticfile.org/vue/2.6.14/vue.min.js"></script>
<link rel="stylesheet" href="https://cdn.staticfile.org/twitter-bootstrap/4.6.0/css/bootstrap.min.css">
```

## 扩展功能

### 添加新的监控指标

1. 在 `MonitorService` 中添加数据收集方法
2. 在 `ApiHandler` 中添加对应的 API 处理
3. 在前端页面中添加展示组件

### 添加告警功能

可以在 `MonitorService` 中添加告警逻辑，例如：
- 节点离线告警
- Leader 变更告警
- 日志积压告警

## 故障排查

### 监控页面无法访问

1. 检查 Raft 节点是否正常启动
2. 检查端口是否被占用
3. 查看日志中的 "Monitor server started" 消息

### 数据显示不正确

1. 刷新页面查看最新数据
2. 检查 API 响应（浏览器开发者工具）
3. 查看 Raft 节点日志

## 开发计划

- [ ] 支持多 Raft 组监控
- [ ] 添加历史数据图表
- [ ] 集成 Prometheus 导出
- [ ] 添加认证授权
- [ ] 支持告警配置
- [ ] SSE 实时推送（替代轮询）
