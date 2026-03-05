package com.zhiyuan.zm.raft.persistence;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.constant.DataOperationType;
import com.zhiyuan.zm.raft.dto.Command;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.MVCCRow;
import com.zhiyuan.zm.raft.dto.MVCCVersion;
import com.zhiyuan.zm.raft.dto.Row;
import com.zhiyuan.zm.raft.util.ByteUtil;
import com.zhiyuan.zm.raft.util.KeyUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;

/**
 *@author zhouzhiyuan
 *@date 2022/4/2
 */
public class DefaultSaveDataImpl implements SaveData {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultSaveDataImpl.class);

  private RocksDB rocksDB;

  /**
   * 获取 RocksDB 实例（用于 MVCC GC 等高级功能）
   * @return RocksDB 实例
   */
  public RocksDB getRocksDB() {
    return rocksDB;
  }

  public DefaultSaveDataImpl(GlobalConfig config) throws RocksDBException {
    File file = new File(config.getDataPath());
    if (!file.exists()) {
      file.mkdirs();
    }
    rocksDB = RocksDB.open(config.getDataPath());
    //writeOptions.setDisableWAL(true);
  }

  @Override
  public void put(byte[] key, byte[] value) throws RocksDBException {
    rocksDB.put(key,value);
  }

  @Override
  public byte[] getValue(byte[] key) throws RocksDBException {
    return rocksDB.get(key);
  }

  @Override
  public void delete(byte[] key) throws RocksDBException {
    rocksDB.delete(key);
  }

  @Override
  public boolean update(byte[] key, byte[] value) throws RocksDBException {

    if (rocksDB.get(key) != null) {
      rocksDB.put(key, value);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public List<Row> scan(byte[] startKey, byte[] endKey) {
    List<Row> rows = new LinkedList<>();
    RocksIterator iterator = rocksDB.newIterator();
    for (iterator.seek(startKey); iterator.isValid(); iterator.next()) {
      byte[] key = iterator.key();
      if (ByteUtil.bytesCompare(key, endKey) < 0) {
        rows.add(new Row(key, iterator.value()));
      }
    }
    return rows;
  }

  @Override
  public void assembleData(WriteBatch batch, LogEntries[] entries, byte[] prefixKey) throws RocksDBException {
    for (int i = 0; i < entries.length; i++) {
      // 使用定制的反序列化方式处理 Command
      String message = entries[i].getMesssage();
      Command command = parseCommand(message);
      if (command == null) {
        LOG.error("解析 Command 失败：" + message);
        continue;
      }
      int cmd = command.getCmd();
      Row[] rows = command.getRows();

      // 处理 MVCC 相关操作
      if (DataOperationType.MVCC_WRITE == cmd) {
        for (Row row : rows) {
          // MVCC 数据的 key 已经包含完整的 MVCC 信息，直接写入
          batch.put(row.getKey(), row.getValue());
        }
      } else if (DataOperationType.MVCC_DELETE == cmd) {
        for (Row row : rows) {
          batch.delete(row.getKey());
        }
      } else if (DataOperationType.INSERT == cmd) {
        for (Row row : rows) {
          batch.put(ByteUtil.concatBytes(prefixKey, row.getKey()), row.getValue());
        }
      } else {
        switch (cmd) {
          case DataOperationType.DELETE:
            for (Row row : rows) {
              batch.delete(ByteUtil.concatBytes(prefixKey, row.getKey()));
            }
            break;
          case DataOperationType.EMPTY:
            break;
          case DataOperationType.TRANSACTION:
              for (Row row : rows) {
                  batch.put(row.getKey(), row.getValue());
              }
            break;
          case DataOperationType.CONFIG_CHANGE:
            break;
          default:
            LOG.debug("Unknown command type: {}", cmd);
        }
      }
    }
  }

  /**
   * 自定义 Command 解析方法，处理 byte[] 反序列化问题
   */
  private Command parseCommand(String message) {
    try {
      return JSON.parseObject(message, Command.class);
    } catch (Exception e) {
      // 如果标准反序列化失败，尝试手动解析
      try {
        com.alibaba.fastjson.JSONObject jsonObject = JSON.parseObject(message);
        int cmd = jsonObject.getIntValue("cmd");
        com.alibaba.fastjson.JSONArray rowsArray = jsonObject.getJSONArray("rows");
        if (rowsArray != null) {
          Row[] rows = new Row[rowsArray.size()];
          for (int i = 0; i < rowsArray.size(); i++) {
            com.alibaba.fastjson.JSONObject rowObj = rowsArray.getJSONObject(i);
            byte[] key = rowObj.getBytes("key");
            byte[] value = rowObj.getBytes("value");
            rows[i] = new Row(key, value);
          }
          return new Command(cmd, rows);
        }
        return new Command(cmd);
      } catch (Exception ex) {
        LOG.error("手动解析 Command 失败", ex);
        return null;
      }
    }
  }


  @Override
  public void writBatch(WriteBatch batch ) throws RocksDBException {
    rocksDB.write(new WriteOptions(), batch);
  }

  @Override
  public void close() {
    if (rocksDB != null) {
      rocksDB.close();
      rocksDB = null;
    }
  }

  // ==================== MVCC 相关方法实现 ====================

  @Override
  public void putMVCC(byte[] key, MVCCVersion mvccVersion) throws RocksDBException {
    byte[] valueBytes = JSON.toJSONBytes(mvccVersion);
    rocksDB.put(key, valueBytes);
  }

  @Override
  public MVCCVersion getMVCC(byte[] key) throws RocksDBException {
    byte[] valueBytes = rocksDB.get(key);
    if (valueBytes == null) {
      return null;
    }
    return JSON.parseObject(new String(valueBytes), MVCCVersion.class);
  }

  @Override
  public List<MVCCVersion> scanMVCCVersions(byte[] startKey, byte[] endKey) {
    List<MVCCVersion> versions = new LinkedList<>();
    RocksIterator iterator = rocksDB.newIterator();
    for (iterator.seek(startKey); iterator.isValid(); iterator.next()) {
      byte[] key = iterator.key();
      if (ByteUtil.bytesCompare(key, endKey) < 0) {
        try {
          MVCCVersion version = JSON.parseObject(new String(iterator.value()), MVCCVersion.class);
          versions.add(version);
        } catch (Exception e) {
          LOG.warn("解析 MVCC 版本失败：key={}", key, e);
        }
      }
    }
    return versions;
  }

  @Override
  public MVCCVersion getMVCCByVersion(int groupId, byte[] userKey, long snapshotTs) throws RocksDBException {
    // 构造范围查询的 startKey 和 endKey
    byte[] startKey = KeyUtil.generateMVCCDataKeyStart(groupId, userKey);
    byte[] endKey = KeyUtil.generateMVCCDataKeyEnd(groupId, userKey);

    MVCCVersion visibleVersion = null;
    RocksIterator iterator = rocksDB.newIterator();

    // 反向迭代，从最大的 transactionId 开始查找
    iterator.seekForPrev(endKey);

    while (iterator.isValid()) {
      byte[] key = iterator.key();
      // 如果 key 小于 startKey，说明已经超出了范围
      if (ByteUtil.bytesCompare(key, startKey) < 0) {
        break;
      }

      try {
        MVCCVersion version = JSON.parseObject(new String(iterator.value()), MVCCVersion.class);

        // 检查版本是否可见：
        // 1. 版本必须已提交
        // 2. 版本的 commitTs 必须 <= snapshotTs
        // 3. 版本不能被删除
        if (version.isCommitted() && version.getCommitTs() <= snapshotTs && !version.isDeleted()) {
          visibleVersion = version;
          break; // 找到第一个可见版本即可（因为是反向迭代，所以是最新版本）
        }
      } catch (Exception e) {
        LOG.warn("解析 MVCC 版本失败：key={}", key, e);
      }

      iterator.prev();
    }

    return visibleVersion;
  }

  @Override
  public void deleteMVCC(byte[] key) throws RocksDBException {
    rocksDB.delete(key);
  }

  @Override
  public void putMVCCBatch(WriteBatch batch, List<MVCCRow> rows, int groupId) throws RocksDBException {
    for (MVCCRow row : rows) {
      byte[] key = KeyUtil.generateMVCCDataKey(groupId, row.getUserKey(), row.getTransactionId());
      byte[] valueBytes = JSON.toJSONBytes(row.getVersion());
      batch.put(key, valueBytes);
    }
  }

  @Override
  public long getLastCommittedTs() throws RocksDBException {
    byte[] key = KeyUtil.generateMVCCLastCommitTsKey();
    byte[] valueBytes = rocksDB.get(key);
    if (valueBytes == null) {
      return 0L;
    }
    try {
      return Long.parseLong(new String(valueBytes));
    } catch (NumberFormatException e) {
      LOG.warn("解析 lastCommittedTs 失败，返回 0", e);
      return 0L;
    }
  }

  @Override
  public void updateLastCommittedTs(long commitTs) throws RocksDBException {
    byte[] key = KeyUtil.generateMVCCLastCommitTsKey();
    byte[] valueBytes = String.valueOf(commitTs).getBytes();
    rocksDB.put(key, valueBytes);
  }
}
