package com.zhiyuan.zm.raft.role.transaction;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zhiyuan.zm.raft.constant.DataOperationType;
import com.zhiyuan.zm.raft.constant.StatusCode;
import com.zhiyuan.zm.raft.dto.Command;
import com.zhiyuan.zm.raft.dto.DataResponest;
import com.zhiyuan.zm.raft.dto.MVCCRow;
import com.zhiyuan.zm.raft.dto.MVCCVersion;
import com.zhiyuan.zm.raft.dto.Row;
import com.zhiyuan.zm.raft.role.LeaderRole;
import com.zhiyuan.zm.raft.util.KeyUtil;

/**
 * MVCC 事务服务
 * 基于 MVCC（多版本并发控制）的事务管理，支持：
 * 1. 快照隔离（Snapshot Isolation）
 * 2. 多事务并发执行
 * 3. 写冲突检测
 * 4. 版本垃圾回收
 *
 * @author zhouzhiyuan
 * @date 2026/03/05
 */
public class MVCCTransactionService {

    private static final Logger LOGGER = LoggerFactory.getLogger(MVCCTransactionService.class);

    /**
     * 全局最后提交时间戳（原子操作）
     */
    private final AtomicLong lastCommittedTs;

    /**
     * 已分配的最大事务 ID
     */
    private volatile long maxTransactionId;

    /**
     * 事务状态管理 - 内存缓存
     */
    private final Map<String, TransactionService.TransactionInfo> transactionStatus = new ConcurrentHashMap<>();

    /**
     * 事务内待提交数据缓存（MVCC 模式）
     * key 为 clientId，value 为待提交的 MVCC 数据行
     */
    private final Map<String, List<MVCCRow>> pendingMVCCWrites = new ConcurrentHashMap<>();

    /**
     * 事务内删除的 key 集合（用于标记删除）
     */
    private final Map<String, Set<byte[]>> pendingDeletes = new ConcurrentHashMap<>();

    /**
     * 事务内数据操作计数
     */
    private final Map<String, AtomicInteger> transactionDataCount = new ConcurrentHashMap<>();

    private final LeaderRole leader;

    public MVCCTransactionService(LeaderRole leader) throws RocksDBException {
        this.leader = leader;

        // 从持久化存储中读取最后提交时间戳
        long storedLastCommittedTs = leader.getSaveData().getLastCommittedTs();
        this.lastCommittedTs = new AtomicLong(storedLastCommittedTs);
        this.maxTransactionId = storedLastCommittedTs;

        LOGGER.info("MVCC TransactionService initialized, lastCommittedTs={}", lastCommittedTs.get());
    }

    /**
     * 获取当前全局最大提交时间戳（用于分配 snapshotTs）
     */
    public long getCurrentCommittedTs() {
        return lastCommittedTs.get();
    }

    /**
     * 生成事务 ID（基于原子自增）
     */
    private synchronized long generateTransactionId() {
        return ++maxTransactionId;
    }

    /**
     * 开启 MVCC 事务
     *
     * @param request 请求标识 (clientId)
     * @return 开启结果
     */
    public DataResponest openTransaction(String request) {
        // 1. 检查请求参数
        if (request == null || request.isEmpty()) {
            LOGGER.error("MVCC: Invalid request (clientId is null or empty)");
            return new DataResponest(StatusCode.SYSTEMEXCEPTION, "Invalid request: clientId is null or empty");
        }

        try {
            // 1. 生成事务 ID
            long transactionId = generateTransactionId();

            // 2. 获取当前全局最大 commitTs 作为 snapshotTs
            //    beginTs 设置为当前最大 transactionId + 1，用于冲突检测
            long snapshotTs = lastCommittedTs.get();
            long beginTs = maxTransactionId + 1;

            TransactionService.TransactionInfo transactionInfo = new TransactionService.TransactionInfo(transactionId, snapshotTs, beginTs);

            // 3. 将事务状态持久化（可选，用于故障恢复）
            byte[] valueBytes = JSON.toJSONBytes(transactionInfo);
            DataResponest dataResponest = leader.setData(JSON.toJSONString(
                    new Command(DataOperationType.INSERT, new Row[]{
                            new Row(KeyUtil.generateTransactionIdKey(transactionId), valueBytes)
                    })
            ));

            if (dataResponest.isSuccess()) {
                dataResponest.setMessage(request);
                // 返回 transactionId 给客户端
                dataResponest.setData(String.valueOf(transactionId));

                // 4. 在内存中注册事务
                transactionStatus.put(request, transactionInfo);
                pendingMVCCWrites.put(request, new LinkedList<>());
                pendingDeletes.put(request, new HashSet<>());
                transactionDataCount.put(request, new AtomicInteger(0));

                LOGGER.info("MVCC: Open transaction success, clientId={}, transactionId={}, snapshotTs={}, beginTs={}",
                        request, transactionId, snapshotTs, beginTs);
                return dataResponest;
            } else {
                LOGGER.error("MVCC: Failed to persist transaction info: {}", dataResponest.getMessage());
                return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
                        "Failed to persist transaction info: " + dataResponest.getMessage());
            }
        } catch (Exception e) {
            LOGGER.error("MVCC: Exception while opening transaction", e);
            return new DataResponest(StatusCode.SYSTEMEXCEPTION, "Exception: " + e.getMessage());
        }
    }

    /**
     * MVCC 快照读
     * 读取在事务 snapshotTs 之前已提交的最大版本
     *
     * @param request 请求标识 (clientId)
     * @param key     要读取的 key
     * @return 读取结果
     */
    public DataResponest getInTransaction(String request, byte[] key) {
        TransactionService.TransactionInfo info = transactionStatus.get(request);
        if (info == null || !info.getStatus().isOpen()) {
            LOGGER.warn("MVCC: Transaction not found or not open for clientId: {}", request);
            return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
                    "Transaction not found or not open");
        }

        try {
            int groupId = leader.getRaftStatus().getGroupId();

            // 1. 检查是否在 writeSet 中有未提交的写入
            //    如果有，返回未提交的值（读已写）
            String userKeyStr = new String(key, StandardCharsets.UTF_8);
            List<MVCCRow> pendingWrites = pendingMVCCWrites.get(request);
            if (pendingWrites != null) {
                for (MVCCRow row : pendingWrites) {
                    if (Arrays.equals(row.getUserKey(), key)) {
                        // 返回未提交的写入（如果未被标记删除）
                        if (!row.isDeleted()) {
                            return new DataResponest(StatusCode.SUCCESS,
                                    new String(row.getValue(), StandardCharsets.UTF_8));
                        } else {
                            return new DataResponest(StatusCode.NOT_FOUND, "Key not found (deleted in transaction)");
                        }
                    }
                }
            }

            // 2. 检查是否在 pendingDeletes 中
            Set<byte[]> deletes = pendingDeletes.get(request);
            if (deletes != null) {
                for (byte[] deletedKey : deletes) {
                    if (Arrays.equals(deletedKey, key)) {
                        return new DataResponest(StatusCode.NOT_FOUND, "Key not found (deleted in transaction)");
                    }
                }
            }

            // 3. 使用 snapshotTs 进行快照读
            MVCCVersion version = leader.getSaveData().getMVCCByVersion(groupId, key, info.getSnapshotTs());

            if (version != null && version.isCommitted()) {
                String value = new String(version.getValue(), StandardCharsets.UTF_8);
                LOGGER.debug("MVCC: Snapshot read success, clientId={}, key={}, snapshotTs={}", request, key, info.getSnapshotTs());
                return new DataResponest(StatusCode.SUCCESS, value);
            } else {
                return new DataResponest(StatusCode.NOT_FOUND, "Key not found");
            }
        } catch (RocksDBException e) {
            LOGGER.error("MVCC: Exception while reading", e);
            return new DataResponest(StatusCode.SYSTEMEXCEPTION, "Exception: " + e.getMessage());
        }
    }

    /**
     * MVCC 写（创建新版本）
     *
     * @param clientId 客户端标识
     * @param key      数据 key
     * @param value    数据 value
     * @return 操作结果
     */
    public DataResponest putInTransaction(String clientId, byte[] key, byte[] value) {
        TransactionService.TransactionInfo info = transactionStatus.get(clientId);
        if (info == null || !info.getStatus().isOpen()) {
            LOGGER.warn("MVCC: Transaction not found or not open for clientId: {}", clientId);
            return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
                    "Transaction not found or not open");
        }

        // 将写入操作缓存到 pendingWrites
        List<MVCCRow> pendingRows = pendingMVCCWrites.get(clientId);
        if (pendingRows == null) {
            pendingRows = new LinkedList<>();
            pendingMVCCWrites.put(clientId, pendingRows);
        }

        // 创建新版本，transactionId 暂为 0，提交时再分配
        MVCCRow mvccRow = new MVCCRow(key, value, 0);
        pendingRows.add(mvccRow);

        // 添加到 writeSet（用于冲突检测）
        String userKeyStr = new String(key, StandardCharsets.UTF_8);
        info.addToWriteSet(userKeyStr);

        // 更新计数
        info.incrementDataCount();
        AtomicInteger count = transactionDataCount.get(clientId);
        if (count != null) {
            count.incrementAndGet();
        }

        LOGGER.debug("MVCC: Put in transaction, clientId={}, key={}", clientId, new String(key, StandardCharsets.UTF_8));
        return new DataResponest(StatusCode.SUCCESS, "Data cached for transaction");
    }

    /**
     * MVCC 删除（标记删除）
     *
     * @param clientId 客户端标识
     * @param key      要删除的 key
     * @return 操作结果
     */
    public DataResponest deleteInTransaction(String clientId, byte[] key) {
        TransactionService.TransactionInfo info = transactionStatus.get(clientId);
        if (info == null || !info.getStatus().isOpen()) {
            LOGGER.warn("MVCC: Transaction not found or not open for clientId: {}", clientId);
            return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
                    "Transaction not found or not open");
        }

        // 将删除操作缓存到 pendingDeletes
        Set<byte[]> deletes = pendingDeletes.get(clientId);
        if (deletes == null) {
            deletes = new HashSet<>();
            pendingDeletes.put(clientId, deletes);
        }
        deletes.add(key);

        // 同时在 pendingWrites 中添加一个删除标记
        List<MVCCRow> pendingRows = pendingMVCCWrites.get(clientId);
        if (pendingRows == null) {
            pendingRows = new LinkedList<>();
            pendingMVCCWrites.put(clientId, pendingRows);
        }

        // 创建删除标记版本
        MVCCRow deleteMarker = new MVCCRow(key, new byte[0], 0);
        deleteMarker.setDeleted(true);
        pendingRows.add(deleteMarker);

        // 添加到 writeSet
        String userKeyStr = new String(key, StandardCharsets.UTF_8);
        info.addToWriteSet(userKeyStr);

        info.incrementDataCount();
        AtomicInteger count = transactionDataCount.get(clientId);
        if (count != null) {
            count.incrementAndGet();
        }

        LOGGER.debug("MVCC: Delete in transaction, clientId={}, key={}", clientId, new String(key, StandardCharsets.UTF_8));
        return new DataResponest(StatusCode.SUCCESS, "Delete cached for transaction");
    }

    /**
     * 提交 MVCC 事务
     * 1. 冲突检测
     * 2. 分配 commitTs
     * 3. 写入所有 pendingWrites
     * 4. 更新全局 lastCommittedTs
     * 5. 更新事务状态
     *
     * @param request 请求标识 (clientId)
     * @return 提交结果
     */
    public DataResponest commitTransaction(String request) {
        TransactionService.TransactionInfo info = transactionStatus.get(request);
        if (info == null) {
            LOGGER.warn("MVCC: Transaction not found for clientId: {}", request);
            return new DataResponest(StatusCode.TRANSACTION_EXCEPTION, "Transaction not found");
        }

        if (!info.getStatus().isOpen()) {
            LOGGER.warn("MVCC: Transaction status is not OPEN: {}", info.getStatus());
            return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
                    "Transaction status is not OPEN: " + info.getStatus());
        }

        try {
            // 1. 冲突检测
            DataResponest conflictResult = checkConflict(info);
            if (!conflictResult.isSuccess()) {
                LOGGER.warn("MVCC: Conflict detected for transaction {}: {}", request, conflictResult.getMessage());
                return conflictResult;
            }

            // 2. 分配 commitTs（全局自增）
            long commitTs = lastCommittedTs.incrementAndGet();
            info.setCommitTs(commitTs);

            // 3. 将所有 pendingWrites 写入 Raft 日志
            List<MVCCRow> pendingRows = pendingMVCCWrites.get(request);
            Set<byte[]> pendingDeleteKeys = pendingDeletes.get(request);

            if ((pendingRows != null && !pendingRows.isEmpty()) ||
                    (pendingDeleteKeys != null && !pendingDeleteKeys.isEmpty())) {

                // 转换为 MVCC 版本并写入
                List<MVCCRow> mvccRows = new LinkedList<>();
                int groupId = leader.getRaftStatus().getGroupId();

                for (MVCCRow row : pendingRows) {
                    // 设置版本信息
                    MVCCVersion version = row.getVersion();
                    version.setTransactionId(info.getTransactionId());
                    version.setCommitTs(commitTs);
                    version.setStatus(MVCCVersion.Status.COMMITTED);

                    mvccRows.add(row);
                }

                // 构建 MVCC 写入命令
                byte[][] mvccKeys = new byte[mvccRows.size()][];
                byte[][] mvccValues = new byte[mvccRows.size()][];

                for (int i = 0; i < mvccRows.size(); i++) {
                    MVCCRow row = mvccRows.get(i);
                    mvccKeys[i] = KeyUtil.generateMVCCDataKey(groupId, row.getUserKey(), info.getTransactionId());
                    mvccValues[i] = JSON.toJSONBytes(row.getVersion());
                }

                Row[] rows = new Row[mvccKeys.length];
                for (int i = 0; i < mvccKeys.length; i++) {
                    rows[i] = new Row(mvccKeys[i], mvccValues[i]);
                }

                Command command = new Command(DataOperationType.MVCC_WRITE, rows);
                DataResponest writeResult = leader.setData(JSON.toJSONString(command));

                if (!writeResult.isSuccess()) {
                    LOGGER.error("MVCC: Failed to write transaction data: {}", writeResult.getMessage());
                    return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
                            "Failed to write transaction data: " + writeResult.getMessage());
                }

                // 4. 更新全局最后提交时间戳持久化
                leader.getSaveData().updateLastCommittedTs(commitTs);
            }

            // 5. 更新事务状态为 CLOSE
            info.setStatus(TransactionService.Status.CLOSE);
            byte[] valueBytes = JSON.toJSONBytes(info);
            DataResponest statusResult = leader.setData(JSON.toJSONString(
                    new Command(DataOperationType.INSERT, new Row[]{
                            new Row(KeyUtil.generateTransactionIdKey(info.getTransactionId()), valueBytes)
                    })
            ));

            // 6. 清理内存缓存
            pendingMVCCWrites.remove(request);
            pendingDeletes.remove(request);
            transactionDataCount.remove(request);
            transactionStatus.remove(request);

            LOGGER.info("MVCC: Commit transaction success, clientId={}, transactionId={}, commitTs={}",
                    request, info.getTransactionId(), commitTs);
            return statusResult;

        } catch (Exception e) {
            LOGGER.error("MVCC: Exception while committing transaction", e);
            return new DataResponest(StatusCode.SYSTEMEXCEPTION, "Exception: " + e.getMessage());
        }
    }

    /**
     * 冲突检测
     * 检查是否有其他事务在 T.beginTs 之后提交了相同的 key
     */
    private DataResponest checkConflict(TransactionService.TransactionInfo info) {
        // 获取所有在 info.beginTs 之后提交的事务
        // 简单实现：扫描所有活跃事务的 writeSet
        // 优化方案：使用更高效的冲突检测机制

        for (Map.Entry<String, TransactionService.TransactionInfo> entry : transactionStatus.entrySet()) {
            TransactionService.TransactionInfo other = entry.getValue();

            // 跳过自己
            if (other.getTransactionId().equals(info.getTransactionId())) {
                continue;
            }

            // 只检查已提交的事务
            if (other.getCommitTs() == 0) {
                continue;
            }

            // 检查是否有写集交集
            if (other.getCommitTs() > info.getBeginTs()) {
                Set<String> intersection = new HashSet<>(info.getWriteSet());
                intersection.retainAll(other.getWriteSet());

                if (!intersection.isEmpty()) {
                    String conflictKeys = String.join(", ", intersection);
                    return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
                            "Write conflict detected on keys: " + conflictKeys);
                }
            }
        }

        return new DataResponest(StatusCode.SUCCESS, "No conflict");
    }

    /**
     * 回滚 MVCC 事务
     * 丢弃所有未提交的版本
     *
     * @param request 请求标识 (clientId)
     * @return 回滚结果
     */
    public DataResponest rollbackTransaction(String request) {
        TransactionService.TransactionInfo info = transactionStatus.get(request);
        if (info == null) {
            LOGGER.warn("MVCC: Transaction not found for clientId: {}", request);
            return new DataResponest(StatusCode.TRANSACTION_EXCEPTION, "Transaction not found");
        }

        if (!info.getStatus().isOpen()) {
            LOGGER.warn("MVCC: Transaction status is not OPEN: {}", info.getStatus());
            return new DataResponest(StatusCode.TRANSACTION_EXCEPTION,
                    "Transaction status is not OPEN: " + info.getStatus());
        }

        try {
            // 1. 更新事务状态为 ROLLBACK
            info.setStatus(TransactionService.Status.ROLLBACK);
            byte[] valueBytes = JSON.toJSONBytes(info);
            DataResponest dataResponest = leader.setData(JSON.toJSONString(
                    new Command(DataOperationType.INSERT, new Row[]{
                            new Row(KeyUtil.generateTransactionIdKey(info.getTransactionId()), valueBytes)
                    })
            ));

            // 2. 清理内存缓存（不需要写入 Raft 日志）
            pendingMVCCWrites.remove(request);
            pendingDeletes.remove(request);
            transactionDataCount.remove(request);
            transactionStatus.remove(request);

            LOGGER.info("MVCC: Rollback transaction success, clientId={}, transactionId={}",
                    request, info.getTransactionId());
            return dataResponest;

        } catch (Exception e) {
            LOGGER.error("MVCC: Exception while rolling back transaction", e);
            return new DataResponest(StatusCode.SYSTEMEXCEPTION, "Exception: " + e.getMessage());
        }
    }

    /**
     * 获取最小活跃事务 ID（用于 GC）
     */
    public long getMinActiveTransactionId() {
        long minId = Long.MAX_VALUE;
        for (TransactionService.TransactionInfo info : transactionStatus.values()) {
            if (info.getStatus().isOpen() && info.getTransactionId() < minId) {
                minId = info.getTransactionId();
            }
        }
        return minId == Long.MAX_VALUE ? maxTransactionId : minId;
    }

    /**
     * 关闭事务服务
     */
    public void shutdown() {
        transactionStatus.clear();
        pendingMVCCWrites.clear();
        pendingDeletes.clear();
        transactionDataCount.clear();
        LOGGER.info("MVCC TransactionService shutdown completed");
    }
}
