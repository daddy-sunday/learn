package org.example;

import org.example.conf.GlobalConfig;
import org.example.raft.dto.LogEntries;
import org.example.raft.persistence.DefaultSaveLogImpl;
import org.example.raft.persistence.SaveLog;
import org.example.raft.util.ByteUtil;
import org.example.raft.util.RaftUtil;
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

  @Test
  public void init() throws RocksDBException {
    GlobalConfig config = new GlobalConfig();
    config.setLogPath("C:\\Users\\zhouz\\Desktop\\raft\\log2");
    saveLog = new DefaultSaveLogImpl(config);
    saveLog.delete(RaftUtil.generateLogKey(1, 7));
  }


  @Test
  public void findAll() throws RocksDBException {
    String logPath = "C:\\Users\\zhouz\\Desktop\\raft\\log";
    System.out.println(logPath);
     traverseLogSave(logPath);
    for (int i = 2; i <= 5; i++) {
      System.out.println(logPath + i);
      traverseLogSave(logPath+i);
    }

    String dataPath = "C:\\Users\\zhouz\\Desktop\\raft\\data";
    System.out.println(dataPath);
    traverseDataSave(dataPath);
    for (int i = 2; i <= 5; i++) {
      System.out.println(dataPath + i);
      traverseDataSave(dataPath+i);
    }
  }

  private void traverseLogSave(String logPath) throws RocksDBException {
    GlobalConfig config = new GlobalConfig();
    config.setLogPath(logPath);
    saveLog = new DefaultSaveLogImpl(config);

    RocksIterator iterator = saveLog.getIterator();
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      System.out.println(TestByteUtil.parseLogKey(iterator.key()) + " = " + new String(iterator.value()));
    }
  }

  private void traverseDataSave(String logPath) throws RocksDBException {
    GlobalConfig config = new GlobalConfig();
    config.setLogPath(logPath);
    saveLog = new DefaultSaveLogImpl(config);
    RocksIterator iterator = saveLog.getIterator();
    for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
      System.out.println(TestByteUtil.parseDataKey(iterator.key()) + " = " + new String(iterator.value()));
    }
  }

  @Test
  public void testPut1() throws RocksDBException {
    saveLog.saveLog(ByteUtil.concatLogId(1, 11), new byte[] {});
  }

  @Test
  public void testGet1() throws RocksDBException {
    byte[] bytes = saveLog.getBytes(ByteUtil.concatLogId(1, 11));
    if (bytes == null) {
      System.out.println("null");
    }else {
      System.out.println("not null");
    }
  }



  @Test
  public void testGet() throws RocksDBException {
    System.out.println(saveLog.get(ByteUtil.concatLogId(1, 7)));
    System.out.println(saveLog.get(ByteUtil.concatLogId(1, 3)));
  }

  @Test
  public void testPut() throws RocksDBException {
    LogEntries addLog = new LogEntries();
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
    LogEntries addLog = new LogEntries();
    addLog.setTerm(117);
    saveLog.saveLog(ByteUtil.concatLogId(1, 116), addLog);
  }

  @Test
  public void testWriteBatch() throws RocksDBException {
    WriteOptions options = new WriteOptions();
    options.setDisableWAL(true);

    LogEntries addLog = new LogEntries();
    addLog.setTerm(117);
    saveLog.saveLog(ByteUtil.concatLogId(1, 117), addLog);
  }


  @Test
  public void testDeleteRange() throws RocksDBException {
    saveLog.deleteRange(ByteUtil.concatLogId(1, 1), ByteUtil.concatLogId(1, 3));
  }

  @Test
  public void testGetMaxIndexLog() {
    System.out.println(saveLog.getMaxLog(ByteUtil.concatLogId(1, 116)).getTerm());
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
  public void testScan() {
  }
}
