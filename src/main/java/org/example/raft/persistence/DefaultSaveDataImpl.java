package org.example.raft.persistence;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.example.conf.GlobalConfig;
import org.example.raft.constant.DataOperationType;
import org.example.raft.dto.Command;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.Row;
import org.example.raft.util.ByteUtil;
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

  private WriteOptions writeOptions;


  public DefaultSaveDataImpl(GlobalConfig config) throws RocksDBException {
    File file = new File(config.getLogPath());
    if (!file.exists()) {
      file.mkdirs();
    }
    rocksDB = RocksDB.open(config.getLogPath());
    writeOptions = new WriteOptions();
    writeOptions.setDisableWAL(true);
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
    if (rocksDB.keyMayExist(key, new StringBuilder())) {
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
        for (int i1 = 0; i1 < rows.length; i1++) {
          batch.put(ByteUtil.concatBytes(prefixKey,rows[i].getKey()),rows[i].getValue());
        }
      } else {
        switch (cmd) {
          case DataOperationType.DELETE:
            for (int i1 = 0; i1 < rows.length; i1++) {
              batch.delete(ByteUtil.concatBytes(prefixKey,rows[i].getKey()));
            }
            break;
          case DataOperationType.EMPTY:
            break;
          default:
        }
      }
    }
  }


  @Override
  public void writBatch(WriteBatch batch ) throws RocksDBException {
    rocksDB.write(writeOptions,batch);
  }
}
