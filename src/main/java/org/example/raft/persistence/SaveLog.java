package org.example.raft.persistence;

import java.util.List;

import org.example.raft.dto.LogEntries;
import org.example.raft.dto.Row;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public interface SaveLog  {

  void saveLog(byte[] key, LogEntries raftLog) throws RocksDBException;

  void saveLog(byte[] key,byte[] value) throws RocksDBException;

  void deleteRange(byte[] start,byte[] end) throws RocksDBException;
  void delete(byte[] key) throws RocksDBException;

  LogEntries getMaxLog(byte[] key);

  LogEntries get(byte[] key) throws RocksDBException;

  byte[] getBytes(byte[] key) throws RocksDBException;

  RocksIterator getIterator();

  List<Row> scan(byte[] startKey, byte[] endKey);

  void  assembleData(WriteBatch batch,byte[] key, LogEntries log)  throws RocksDBException;

  void writBatch(WriteBatch batch) throws RocksDBException;
}
