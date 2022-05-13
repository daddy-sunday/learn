package org.example;

import java.util.List;

import org.example.conf.GlobalConfig;
import org.example.raft.dto.AddLogRequest;
import org.example.raft.dto.Row;
import org.example.raft.persistence.DefaultSaveLogImpl;
import org.example.raft.persistence.SaveLog;
import org.example.raft.util.ByteUtil;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteOptions;

/**
 *@author zhouzhiyuan
 *@date 2021/11/30
 */
public class SaveLogTest {

  private SaveLog saveLog;

  @Before
  public void init() throws RocksDBException {
    GlobalConfig config = new GlobalConfig();
    config.setLogPath("C:\\Users\\zhouz\\Desktop\\raft\\test");
    saveLog = new DefaultSaveLogImpl(config);
  }

  @Test
  public void testGet() throws RocksDBException {
    System.out.println(saveLog.get(ByteUtil.concatLogId(1, 7)));
    System.out.println(saveLog.get(ByteUtil.concatLogId(1, 3)));
  }

  @Test
  public void testPut() throws RocksDBException {
    AddLogRequest addLog = new AddLogRequest();
    addLog.setTerm(11L);
    saveLog.saveLog(ByteUtil.concatLogId(1, 11), addLog);
    addLog.setTerm(12);
    saveLog.saveLog(ByteUtil.concatLogId(1, 12), addLog);
    addLog.setTerm(13);
    saveLog.saveLog(ByteUtil.concatLogId(1, 13), addLog);
    addLog.setTerm(14);
    saveLog.saveLog(ByteUtil.concatLogId(1, 14), addLog);
    addLog.setTerm(15);
    saveLog.saveLog(ByteUtil.concatLogId(1, 15), addLog);
    addLog.setTerm(16);
    saveLog.saveLog(ByteUtil.concatLogId(1, 16), addLog);
  }

  @Test
  public void testPutReplace() throws RocksDBException {
    AddLogRequest addLog = new AddLogRequest();
    addLog.setTerm(117);
    saveLog.saveLog(ByteUtil.concatLogId(1, 116), addLog);
  }

  @Test
  public void testWriteBatch() throws RocksDBException {
    WriteOptions options = new WriteOptions();
    options.setDisableWAL(true);

    AddLogRequest addLog = new AddLogRequest();
    addLog.setTerm(117);
    saveLog.saveLog(ByteUtil.concatLogId(1, 117), addLog);
  }


  @Test
  public void testDeleteRange() throws RocksDBException {
    saveLog.deleteRange(ByteUtil.concatLogId(1, 1), ByteUtil.concatLogId(1, 3));
  }

  @Test
  public void testGetMaxIndexLog() {
    System.out.println(saveLog.getMaxLog().getTerm());
  }

  @Test
  public void testGetIterator() {
    RocksIterator iterator = saveLog.getIterator();
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      System.out.println(ByteUtil.parse17(iterator.key()) + " = " + new String(iterator.value()));
    }
  }

  @Test
  public void testGetIteratorSeek() {
    RocksIterator iterator = saveLog.getIterator();
    iterator.seek(ByteUtil.concatLogId(1, 6));
    System.out.println(ByteUtil.parse17(iterator.key()) + " = " + new String(iterator.value()));
  }
  @Test
  public void testGetIteratorSeek1() {
    RocksIterator iterator = saveLog.getIterator();
    //存在返回当前指定的key，不存在指向下一条
    iterator.seek(ByteUtil.concatLogId(1, 7));
    System.out.println(ByteUtil.parse17(iterator.key()) + " = " + new String(iterator.value()));
  }

  @Test
  public void testGetIteratorSeekForPriv() {
    RocksIterator iterator = saveLog.getIterator();
    //存在返回当前指定的key，不存指向上一条
    iterator.seekForPrev(ByteUtil.concatLogId(1, 6));
    System.out.println(ByteUtil.parse17(iterator.key()) + " = " + new String(iterator.value()));
  }
  @Test
  public void testGetIteratorSeekForPriv1() {
    RocksIterator iterator = saveLog.getIterator();
    iterator.seekForPrev(ByteUtil.concatLogId(1, 7));
    System.out.println(ByteUtil.parse17(iterator.key()) + " = " + new String(iterator.value()));
  }

  @Test
  public void testScan(){
    List<Row> scan = saveLog.scan(ByteUtil.longToBytes(1),ByteUtil.longToBytes(2));
    for (Row row : scan) {
      System.out.println(ByteUtil.parse17(row.getKey()) + " = " + new String(row.getValue()));
    }
  }
}
