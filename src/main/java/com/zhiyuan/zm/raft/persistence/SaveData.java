package com.zhiyuan.zm.raft.persistence;

import java.util.List;

import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.MVCCRow;
import com.zhiyuan.zm.raft.dto.MVCCVersion;
import com.zhiyuan.zm.raft.dto.Row;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public interface SaveData {

  void put(byte[] key,byte[] value) throws RocksDBException;

  byte[] getValue(byte[] key) throws RocksDBException;

  void delete(byte[] key) throws RocksDBException;

  boolean update(byte[] key, byte[] value) throws RocksDBException;

  List<Row> scan(byte[] startKey, byte[] endKey);

  void assembleData(WriteBatch batch, LogEntries[] log, byte[] prefixKey) throws RocksDBException;

  void writBatch(WriteBatch batch) throws RocksDBException;

  /**
   * 关闭 RocksDB 资源
   */
  void close();

  // ==================== MVCC 相关方法 ====================

  /**
   * 写入 MVCC 数据版本
   * @param key 完整的 MVCC key（包含 transactionId）
   * @param mvccVersion MVCC 版本数据
   * @throws RocksDBException RocksDB 异常
   */
  void putMVCC(byte[] key, MVCCVersion mvccVersion) throws RocksDBException;

  /**
   * 读取 MVCC 数据（单个版本）
   * @param key 完整的 MVCC key
   * @return MVCC 版本数据
   * @throws RocksDBException RocksDB 异常
   */
  MVCCVersion getMVCC(byte[] key) throws RocksDBException;

  /**
   * 扫描某个 userKey 的所有 MVCC 版本（范围查询）
   * @param startKey 起始 key（包含 userKey 前缀）
   * @param endKey 结束 key（包含 userKey 前缀 + 最大 transactionId）
   * @return MVCC 版本列表（按 transactionId 排序）
   */
  List<MVCCVersion> scanMVCCVersions(byte[] startKey, byte[] endKey);

  /**
   * 读取某个 key 在指定 snapshotTs 之前已提交的最大版本（快照读）
   * @param groupId Raft 组 ID
   * @param userKey 用户数据 key
   * @param snapshotTs 快照时间戳
   * @return 可见的 MVCC 版本，如果不存在则返回 null
   * @throws RocksDBException RocksDB 异常
   */
  MVCCVersion getMVCCByVersion(int groupId, byte[] userKey, long snapshotTs) throws RocksDBException;

  /**
   * 删除 MVCC 数据版本
   * @param key 完整的 MVCC key
   * @throws RocksDBException RocksDB 异常
   */
  void deleteMVCC(byte[] key) throws RocksDBException;

  /**
   * 批量写入 MVCC 数据
   * @param batch WriteBatch
   * @param rows MVCC 数据行
   * @param groupId Raft 组 ID
   * @throws RocksDBException RocksDB 异常
   */
  void putMVCCBatch(WriteBatch batch, List<MVCCRow> rows, int groupId) throws RocksDBException;

  /**
   * 获取全局最大提交时间戳
   * @return 最大提交时间戳
   * @throws RocksDBException RocksDB 异常
   */
  long getLastCommittedTs() throws RocksDBException;

  /**
   * 更新全局最大提交时间戳
   * @param commitTs 新的提交时间戳
   * @throws RocksDBException RocksDB 异常
   */
  void updateLastCommittedTs(long commitTs) throws RocksDBException;
}
