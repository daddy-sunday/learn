package com.zhiyuan.zm.raft.persistence;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import com.zhiyuan.zm.conf.GlobalConfig;
import com.zhiyuan.zm.raft.constant.DataOperationType;
import com.zhiyuan.zm.raft.dto.Command;
import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.dto.Row;
import com.zhiyuan.zm.raft.util.ByteUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.alibaba.fastjson.JSON;

/**
 *@author zhouzhiyuan
 *@date 2022/4/2
 */
public class DefaultSaveDataImpl implements SaveData {

  private RocksDB rocksDB;

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
      Command command = JSON.parseObject(entries[i].getMesssage(), Command.class);
      int cmd = command.getCmd();
      Row[] rows = command.getRows();
      if (DataOperationType.INSERT == cmd) {
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
            break;
          case DataOperationType.CONFIG_CHANGE:
            break;
          default:
        }
      }
    }
  }


  @Override
  public void writBatch(WriteBatch batch ) throws RocksDBException {
    rocksDB.write(new WriteOptions(), batch);
  }
}
