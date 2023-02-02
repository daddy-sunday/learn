package com.zhiyuan.zm.raft.persistence;

import com.zhiyuan.zm.raft.util.ByteUtil;
import org.rocksdb.RocksIterator;

/**
 *@author zhouzhiyuan
 *@date 2022/5/26
 */
public class SaveIterator {

  private RocksIterator iterator;

  private byte[] startKey;

  private byte[] endKey;

  public SaveIterator(RocksIterator iterator, byte[] startKey, byte[] endKey) {
    this.iterator = iterator;
    this.startKey = startKey;
    this.endKey = endKey;
  }

  public void seek() {
    iterator.seek(startKey);
  }

  public void next() {
    iterator.next();
  }

  public boolean isValied() {
    if (iterator.isValid()) {
      iterator.key();
      if (ByteUtil.bytesCompare(iterator.key(), endKey) <= 0) {
        return true;
      }else {
        return false;
      }
    }
    return false;
  }

  public byte[] getValue() {
    return iterator.value();
  }
}