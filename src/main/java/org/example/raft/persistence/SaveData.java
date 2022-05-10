package org.example.raft.persistence;

import java.util.List;

import org.example.raft.dto.AddLog;
import org.example.raft.dto.Row;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 *@author zhouzhiyuan
 *@date 2021/10/22
 */
public interface SaveData {

  byte[] getValue(byte[] key) throws RocksDBException;

  void delete(byte[] key) throws RocksDBException;

  boolean update(byte[] key, byte[] value) throws RocksDBException;

  List<Row> scan(byte[] startKey, byte[] endKey);

  void assembleData(WriteBatch batch, AddLog log) throws RocksDBException;

  void writBatch(WriteBatch batch) throws RocksDBException;
}