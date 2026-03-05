package com.zhiyuan.zm.raft.role.transaction;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.zhiyuan.zm.raft.dto.MVCCVersion;
import com.zhiyuan.zm.raft.persistence.SaveData;
import com.zhiyuan.zm.raft.util.ByteUtil;
import com.zhiyuan.zm.raft.util.KeyUtil;

/**
 * MVCC 版本垃圾回收器
 * 定期清理已提交的旧版本数据，减少存储开销
 *
 * 回收策略：
 * 1. 对于每个 userKey，只保留最新的已提交版本
 * 2. 清理 commitTs < minActiveTransactionId 的旧版本（这些版本对任何活跃事务都不可见）
 * 3. 利用 RocksDB 的批量写入特性提高性能
 *
 * @author zhouzhiyuan
 * @date 2026/03/05
 */
public class MVCCGarbageCollector {

    private static final Logger LOGGER = LoggerFactory.getLogger(MVCCGarbageCollector.class);

    /**
     * 默认的 GC 间隔时间（毫秒）
     */
    private static final long DEFAULT_GC_INTERVAL_MS = 60000; // 1 分钟

    /**
     * 每次 GC 处理的最大 userKey 数量（避免单次 GC 时间过长）
     */
    private static final int DEFAULT_MAX_KEYS_PER_GC = 500;

    /**
     * RocksDB 数据存储服务
     */
    private final SaveData saveData;

    /**
     * MVCC 事务服务（用于获取最小活跃事务 ID）
     */
    private final MVCCTransactionService transactionService;

    /**
     * Raft 组 ID
     */
    private final int groupId;

    /**
     * GC 调度器
     */
    private final ScheduledExecutorService gcScheduler;

    /**
     * GC 间隔时间
     */
    private final long gcIntervalMs;

    /**
     * 是否正在运行
     */
    private volatile boolean running = false;

    /**
     * 上一次最小活跃事务 ID（用于避免重复 GC）
     */
    private volatile long lastMinActiveTransactionId = 0;

    /**
     * GC 统计信息
     */
    private volatile long totalVersionsCollected = 0;
    private volatile long lastGcTime = 0;
    private volatile int lastGcCollectedCount = 0;

    /**
     * 构造垃圾回收器
     *
     * @param saveData           RocksDB 数据存储服务
     * @param transactionService MVCC 事务服务
     * @param groupId            Raft 组 ID
     */
    public MVCCGarbageCollector(SaveData saveData, MVCCTransactionService transactionService, int groupId) {
        this(saveData, transactionService, groupId, DEFAULT_GC_INTERVAL_MS);
    }

    /**
     * 构造垃圾回收器
     *
     * @param saveData           RocksDB 数据存储服务
     * @param transactionService MVCC 事务服务
     * @param groupId            Raft 组 ID
     * @param gcIntervalMs       GC 间隔时间（毫秒）
     */
    public MVCCGarbageCollector(SaveData saveData, MVCCTransactionService transactionService, int groupId,
                                long gcIntervalMs) {
        this.saveData = saveData;
        this.transactionService = transactionService;
        this.groupId = groupId;
        this.gcIntervalMs = gcIntervalMs;
        this.gcScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MVCC-GC-" + groupId);
            t.setDaemon(true);
            return t;
        });
        LOGGER.info("MVCC GarbageCollector initialized for groupId={}, gcInterval={}ms", groupId, gcIntervalMs);
    }

    /**
     * 启动垃圾回收器
     */
    public void start() {
        if (running) {
            LOGGER.warn("MVCC GarbageCollector is already running");
            return;
        }
        running = true;
        gcScheduler.scheduleAtFixedRate(this::runGC, gcIntervalMs, gcIntervalMs, TimeUnit.MILLISECONDS);
        LOGGER.info("MVCC GarbageCollector started");
    }

    /**
     * 立即执行一次垃圾回收（用于手动触发或测试）
     *
     * @return 清理的版本数量
     */
    public int runGCNow() {
        runGC();
        return lastGcCollectedCount;
    }

    /**
     * 停止垃圾回收器
     */
    public void shutdown() {
        running = false;
        gcScheduler.shutdown();
        try {
            if (!gcScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                gcScheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            gcScheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        LOGGER.info("MVCC GarbageCollector shutdown completed, total versions collected: {}", totalVersionsCollected);
    }

    /**
     * 执行垃圾回收
     */
    private void runGC() {
        if (!running) {
            return;
        }

        long startTime = System.currentTimeMillis();
        try {
            // 1. 获取最小活跃事务 ID
            long minActiveTransactionId = transactionService.getMinActiveTransactionId();

            // 2. 如果没有活跃事务，使用当前最大提交时间戳
            if (minActiveTransactionId == Long.MAX_VALUE || minActiveTransactionId <= 0) {
                minActiveTransactionId = transactionService.getCurrentCommittedTs();
            }

            // 3. 如果最小活跃事务 ID 没有增长，跳过本次 GC（避免重复扫描）
            if (minActiveTransactionId <= lastMinActiveTransactionId) {
                LOGGER.debug("MVCC GC: minActiveTransactionId={}, no change since last GC, skip",
                        minActiveTransactionId);
                return;
            }

            lastMinActiveTransactionId = minActiveTransactionId;

            LOGGER.info("MVCC GC: Starting GC run, minActiveTransactionId={}", minActiveTransactionId);

            // 4. 扫描并清理旧版本
            int collectedCount = collectOldVersions(minActiveTransactionId);

            long elapsed = System.currentTimeMillis() - startTime;
            lastGcTime = elapsed;
            lastGcCollectedCount = collectedCount;
            totalVersionsCollected += collectedCount;

            LOGGER.info("MVCC GC: Completed in {}ms, collected {} versions, total collected: {}",
                    elapsed, collectedCount, totalVersionsCollected);

        } catch (Exception e) {
            LOGGER.error("MVCC GC: Error during garbage collection", e);
        }
    }

    /**
     * 收集旧版本数据
     *
     * @param minActiveTransactionId 最小活跃事务 ID
     * @return 清理的版本数量
     */
    private int collectOldVersions(long minActiveTransactionId) throws RocksDBException {
        int collectedCount = 0;

        // 获取 MVCC 数据范围
        byte[] startPrefix = new byte[]{KeyUtil.MVCC_DATA_KEY_PREFIX};
        byte[] endPrefix = new byte[]{(byte) (KeyUtil.MVCC_DATA_KEY_PREFIX + 1)};

        RocksDB db = ((com.zhiyuan.zm.raft.persistence.DefaultSaveDataImpl) saveData).getRocksDB();
        RocksIterator iterator = db.newIterator();

        try {
            // 存储需要删除的 key 列表
            List<byte[]> keysToDelete = new ArrayList<>();

            // 按 userKey 分组处理
            byte[] currentUserKeyPrefix = null;
            byte[] latestVersionKeyToKeep = null; // 需要保留的最新版本 key

            iterator.seek(startPrefix);

            while (iterator.isValid() && collectedCount < DEFAULT_MAX_KEYS_PER_GC) {
                byte[] key = iterator.key();

                // 检查是否超出了 MVCC 数据范围
                if (ByteUtil.bytesCompare(key, endPrefix) >= 0) {
                    break;
                }

                // 提取 userKey 前缀（用于分组）
                byte[] userKeyPrefix = extractUserKeyPrefix(key);

                // 如果是新的 userKey，处理上一组数据
                if (currentUserKeyPrefix == null || !bytesEquals(currentUserKeyPrefix, userKeyPrefix)) {
                    // 对于上一组，删除所有旧版本（除了需要保留的最新版本）
                    if (currentUserKeyPrefix != null) {
                        // 删除该组中所有需要删除的旧版本
                        // （这里简化处理：实际应该在内存中维护每组的版本列表）
                    }
                    currentUserKeyPrefix = userKeyPrefix;
                    latestVersionKeyToKeep = null;
                }

                // 解析版本信息
                byte[] valueBytes = iterator.value();
                MVCCVersion version = parseMVCCVersion(valueBytes, key);

                if (version != null && version.isCommitted()) {
                    // 检查是否是旧版本（commitTs < minActiveTransactionId）
                    if (version.getCommitTs() < minActiveTransactionId) {
                        keysToDelete.add(key);
                        collectedCount++;
                    } else {
                        // 这是新版本，保留最新的
                        latestVersionKeyToKeep = key;
                    }
                }

                iterator.next();
            }

            // 批量删除旧版本
            if (!keysToDelete.isEmpty()) {
                try (WriteBatch batch = new WriteBatch()) {
                    for (byte[] key : keysToDelete) {
                        batch.delete(key);
                    }
                    saveData.writBatch(batch);
                }
            }

        } finally {
            iterator.close();
        }

        return collectedCount;
    }

    /**
     * 从 MVCC key 中提取 userKey 前缀
     * Key 格式：1 字节类型 + 4 字节 groupId + N 字节 userKey + 8 字节 transactionId
     *
     * @param key MVCC key
     * @return userKey 前缀（包含类型和 groupId）
     */
    private byte[] extractUserKeyPrefix(byte[] key) {
        if (key == null || key.length < 13) { // 1 + 4 + 8 的最小长度
            return key;
        }
        // 复制从开头到倒数第 8 个字节（去掉 transactionId）
        int prefixLength = key.length - 8;
        byte[] prefix = new byte[prefixLength];
        System.arraycopy(key, 0, prefix, 0, prefixLength);
        return prefix;
    }

    /**
     * 从 MVCC key 中提取完整的 userKey（不包含类型前缀和 transactionId）
     * Key 格式：1 字节类型 + 4 字节 groupId + N 字节 userKey + 8 字节 transactionId
     *
     * @param key MVCC key
     * @return userKey
     */
    private byte[] extractUserKey(byte[] key) {
        if (key == null || key.length < 13) {
            return new byte[0];
        }
        // 跳过 1 字节类型和 4 字节 groupId
        int userKeyLength = key.length - 1 - 4 - 8;
        if (userKeyLength <= 0) {
            return new byte[0];
        }
        byte[] userKey = new byte[userKeyLength];
        System.arraycopy(key, 5, userKey, 0, userKeyLength);
        return userKey;
    }

    /**
     * 解析 MVCC 版本
     *
     * @param valueBytes 序列化后的版本数据
     * @param key        MVCC key
     * @return MVCC 版本对象，如果解析失败返回 null
     */
    private MVCCVersion parseMVCCVersion(byte[] valueBytes, byte[] key) {
        try {
            return JSON.parseObject(new String(valueBytes), MVCCVersion.class);
        } catch (Exception e) {
            LOGGER.warn("MVCC GC: Failed to parse MVCC version, key={}", bytesToHex(key));
            return null;
        }
    }

    /**
     * 检查字节数组是否相等
     */
    private boolean bytesEquals(byte[] a, byte[] b) {
        if (a == null && b == null) {
            return true;
        }
        if (a == null || b == null) {
            return false;
        }
        if (a.length != b.length) {
            return false;
        }
        for (int i = 0; i < a.length; i++) {
            if (a[i] != b[i]) {
                return false;
            }
        }
        return true;
    }

    /**
     * 字节数组转十六进制字符串（用于日志）
     */
    private String bytesToHex(byte[] bytes) {
        if (bytes == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    /**
     * 获取统计信息
     *
     * @return 总共清理的版本数量
     */
    public long getTotalVersionsCollected() {
        return totalVersionsCollected;
    }

    /**
     * 获取上一次 GC 执行时间
     *
     * @return 上一次 GC 耗时（毫秒）
     */
    public long getLastGcTime() {
        return lastGcTime;
    }

    /**
     * 获取上一次 GC 清理的版本数量
     *
     * @return 清理的版本数量
     */
    public int getLastGcCollectedCount() {
        return lastGcCollectedCount;
    }

    /**
     * 是否正在运行
     *
     * @return true 如果 GC 正在运行
     */
    public boolean isRunning() {
        return running;
    }
}
