package org.example.raft.persistence;

import java.io.File;
import java.util.LinkedList;
import java.util.List;

import org.example.conf.GlobalConfig;
import org.example.raft.dto.LogEntries;
import org.example.raft.dto.Row;
import org.example.raft.util.ByteUtil;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.alibaba.fastjson.JSON;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public class DefaultSaveLogImpl implements SaveLog {

  private RocksDB rocksDB;

  private WriteOptions writeOptions;

  public DefaultSaveLogImpl(GlobalConfig config) throws RocksDBException {
    File file = new File(config.getLogPath());
    if (!file.exists()) {
      file.mkdirs();
    }
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMergeOperatorName("put");
    rocksDB = RocksDB.open(options, config.getLogPath());
    writeOptions = new WriteOptions();
    //writeOptions.setDisableWAL(true);
  }

  @Override
  public void saveLog(byte[] key, LogEntries raftLog) throws RocksDBException {
    rocksDB.put(writeOptions,key, JSON.toJSONBytes(raftLog));
  }

  @Override
  public void saveLog(byte[] key, byte[] value) throws RocksDBException {
    rocksDB.put(writeOptions,key, value);
  }

  @Override
  public void deleteRange(byte[] start, byte[] end) throws RocksDBException {
    rocksDB.deleteRange(writeOptions,start, end);
  }

  @Override
  public void delete(byte[] key) throws RocksDBException {
    rocksDB.delete(key);
  }

  @Override
  public LogEntries getMaxLog(byte[] key) {
    RocksIterator rocksIterator = rocksDB.newIterator();
    rocksIterator.seekToLast();
    return JSON.parseObject(rocksIterator.value(), LogEntries.class);
  }

  @Override
  public LogEntries get(byte[] key) throws RocksDBException {
    byte[] bytes = rocksDB.get(key);
    if (bytes == null) {
      return null;
    }
    return JSON.parseObject(bytes, LogEntries.class);
  }

  @Override
  public byte[] getBytes(byte[] key) throws RocksDBException {
    return rocksDB.get(key);
  }

  @Override
  public RocksIterator getIterator() {
    return rocksDB.newIterator();
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
  public void assembleData(WriteBatch batch,byte[] key, LogEntries log) throws RocksDBException {
    batch.put(key, JSON.toJSONBytes(log));
  }

  @Override
  public void writBatch(WriteBatch batch ) throws RocksDBException {
    rocksDB.write(writeOptions,batch);
  }
}
