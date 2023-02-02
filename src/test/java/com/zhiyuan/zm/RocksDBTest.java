package com.zhiyuan.zm;

import java.io.File;

import com.zhiyuan.zm.conf.GlobalConfig;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.Snapshot;

/**
 * @author zhouzhiyuan
 * @date 2022/11/17 9:54
 */
public class RocksDBTest {

  private RocksDB rocksDB;

  public RocksDBTest(GlobalConfig config) throws RocksDBException {
    File file = new File(config.getLogPath());
    if (!file.exists()) {
      file.mkdirs();
    }
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setMergeOperatorName("put");
    rocksDB = RocksDB.open(options, config.getLogPath());
  }

  @Test
  private void snapshaot(){
    Snapshot snapshot = rocksDB.getSnapshot();

  }


}
