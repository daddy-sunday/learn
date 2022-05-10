package org.example;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.example.raft.constant.TaskType;
import org.example.raft.dto.LogEntry;
import org.example.raft.util.ByteUtil;
import org.junit.Test;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

/**
 * Unit test for simple App.
 */
public class AppTest {
  /**
   * Rigorous Test :-)
   */
  @Test
  public void server() throws InterruptedException {
  }

  @Test
  public void TestByteBuffer() {
    long b = 102;
    byte[] bytes = "我是中国人.png".getBytes(StandardCharsets.UTF_8);
    System.out.println(bytes.length);
    ByteBuffer byteBuffer = ByteBuffer.allocate(12 + bytes.length);
    byteBuffer.putLong(b);
    byteBuffer.putInt(bytes.length);
    byteBuffer.put(bytes);
    byte[] array = byteBuffer.array();
    System.out.println(array.length);
    byteBuffer.flip();
    System.out.println(byteBuffer.getLong());
    byte[] name = new byte[byteBuffer.getInt()];
    byteBuffer.get(name);
    System.out.println(new String(name, StandardCharsets.UTF_8));
  }


  private long szie = 10;

  @Test
  public void testUnsafe() {
    System.out.println(0 % (300 - 150 + 1) + 150);
  }


  @Test
  public void testEfficient2() {
    LogEntry[] raftLogs = new LogEntry[1000000];
    LogEntry log = new LogEntry();
    log.setCmd(1000000);
    long startTime = System.currentTimeMillis();
    Integer a;
    Integer b = log.getCmd();
    for (LogEntry raftLog : raftLogs) {
      if (true) {
        a = log.getCmd();
        a = log.getCmd();
        a = log.getCmd();
        a = log.getCmd();
        a = log.getCmd();
      }
    }
    long endTime = System.currentTimeMillis();
    System.out.println(endTime - startTime);
  }

  /**
   * 直接引用>类名.引用>get set 方法返回引用
   */
  @Test
  public void testEfficient3() {
    LogEntry[] raftLogs = new LogEntry[1000000];
    LogEntry log = new LogEntry();
    log.setCmd(1000000);
    long startTime = System.currentTimeMillis();
    Integer a;
    int b = log.getCmd();
    for (LogEntry raftLog : raftLogs) {
      if (true) {
        a = raftLogs.length;
        a = raftLogs.length;
        a = raftLogs.length;
        a = raftLogs.length;
        a = raftLogs.length;
      }
    }
    long endTime = System.currentTimeMillis();
    System.out.println(endTime - startTime);
  }

  @Test
  public void testRocksDB() throws RocksDBException {
    Options options = new Options();
    options.setCreateIfMissing(true);
    options.setCreateMissingColumnFamilies(true);
    RocksDB rocksDB = RocksDB.open(options, "C:\\Users\\zhouz\\Desktop\\raft\\test");

    rocksDB.put("小明".getBytes(), "人类".getBytes());
    WriteBatch writeBatch = new WriteBatch();


    System.out.println(rocksDB.get("小明".getBytes()));
    System.out.println(rocksDB.get("1111122".getBytes()));
  }


  @Test
  public void testThreadPollExecutor() {
    int a =3,b=2;
    System.out.println(a-b<b);
  }

  @Test
  public void client() {

    ExecutorService executor = Executors.newFixedThreadPool(10);

    List<SendMessage> list = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      list.add(new SendMessage("线程 " + i));
    }

    List<Future<Boolean>> result = null;
    for (int i = 0; i < 10; i++) {
      long starTime = System.currentTimeMillis();
      try {
        result = executor.invokeAll(list, 2000, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      int voteResult = 0;

      for (Future<Boolean> booleanFuture : result) {
        try {
          if (booleanFuture.get()) {
            voteResult++;
          }
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      long endTime = System.currentTimeMillis();
      System.out.println("完成值为：" + voteResult+" 完成时间"+(endTime-starTime));
    }

  }

  class SendMessage implements Callable<Boolean> {
    private String threadName;

    public SendMessage(String threadName) {
      this.threadName = threadName;
    }

    @Override
    public Boolean call() {
     /* RaftRpcRequest rpcRequest = new RaftRpcRequest();
      rpcRequest.setType(threadName);
      RpcClient rpcClient = new RpcClient();
      rpcClient.init();
      for (int i = 0; i < 1000; i++) {
        try {
          Object o = rpcClient.invokeSync("127.0.0.1:8080", rpcRequest, 1000);
          System.out.println(threadName + " 发送了消息");
          Thread.sleep(1000);
        } catch (RemotingException e) {
          e.printStackTrace();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }*/
      try {
        Thread.sleep(getVoteTimeOut());
      } catch (InterruptedException e) {
      }
      return true;
    }
  }


  /**
   * 随机151-300
   * @return
   */
  private int getVoteTimeOut() {
    Random r = new Random();
    return r.nextInt(3000) % (3000 - 1500 + 1) + 1500;
  }


  @Test
  public void testStringCompareTo() {

    System.out.println(stringComparaTo("abbbb".getBytes(), "b".getBytes()));
    System.out.println(stringComparaTo("d".getBytes(), "c".getBytes()));
    System.out.println(stringComparaTo(ByteUtil.concatLogId(1, 11), ByteUtil.concatLogId(1, 12)));
    System.out.println(stringComparaTo(ByteUtil.concatLogId(1, 11), ByteUtil.concatLogId(1, 1)));

    System.out.println(bytesComparaTo("abb".getBytes(), "b".getBytes()));
    System.out.println(bytesComparaTo("d".getBytes(), "c".getBytes()));
    System.out.println(bytesComparaTo(ByteUtil.concatLogId(1, 11), ByteUtil.concatLogId(1, 12)));
    System.out.println(bytesComparaTo(ByteUtil.concatLogId(1, 11), ByteUtil.concatLogId(1, 1)));
  }


  public int stringComparaTo(byte[] param1, byte[] param2) {
    String s = param1.toString();
    String s1 = param2.toString();
    return s.compareTo(s1);
  }

  public int bytesComparaTo(byte[] param1, byte[] param2) {
    int len1 = param1.length;
    int len2 = param2.length;
    int lim = Math.min(len1, len2);
    byte v1[] = param1;
    byte v2[] = param2;

    int k = 0;
    while (k < lim) {
      byte c1 = v1[k];
      byte c2 = v2[k];
      if (c1 != c2) {
        return c1 - c2;
      }
      k++;
    }
    return len1 - len2;
  }

  @Test
  public void switchTest(){
    int a = 1;
    switch (a){
      case 2:
        System.out.println(111);
        break;
      case 3:
        System.out.println(2222);
        break;
      default:
    }

  }
  @Test
  public void testMapSzie(){
    System.out.println(tableSizeFor(32));
  }

  /**
   * Returns a power of two size for the given target capacity.
   */
   public int tableSizeFor(int cap) {
    int n = cap - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 :  n + 1;
  }

  @Test
  public void testGetParentPath(){
     String testPath1 = "/aaa/bbb/cccc/txt.a";
     String testPath2 = "/aaa";
     String testPath3 = "/aaa/bbb";
    String testPath4 = null;
    System.out.println(getParentPath(testPath1));
    System.out.println(getParentPath(testPath2));
    System.out.println(getParentPath(testPath3));
    System.out.println(getParentPath(testPath4));
  }
  String linux_separator= "/";

  public static String getParentPath(String path){
    if (path == null) {
      return null;
    }
    if (path.split("/").length<3) {
      return path;
    }
    return path.substring(0,path.lastIndexOf("/"));
  }

  @Test
  public void testReference(){
    AtomicInteger count = new AtomicInteger();
    System.out.println(count.addAndGet(1));
    System.out.println(count.addAndGet(1));
    System.out.println(count.addAndGet(1));
    AtomicReference<String> result = new AtomicReference<>();
    System.out.println(result.get());
    System.out.println(TaskType.XIAOLI.toString());
  }

  @Test
  public void IntegerValue(){
    System.out.println(Long.MAX_VALUE);
  }



}
