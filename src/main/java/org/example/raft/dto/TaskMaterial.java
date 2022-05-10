package org.example.raft.dto;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 *@author zhouzhiyuan
 *@date 2022/4/27
 */
public class TaskMaterial {

  private AddLog addLog;

  private CountDownLatch countDownLatch;

  private AtomicInteger count;

  /**
   *  //一批数据的最后一个元素 ，= true 时更新 commitLogIndex
   */
  private volatile boolean commitLogIndexFlag = false;

  private volatile AddLog[] addLogs;

  public TaskMaterial(AddLog addLog, CountDownLatch countDownLatch, AtomicInteger count) {
    this.addLog = addLog;
    this.countDownLatch = countDownLatch;
    this.count = count;
  }

  public AddLog[] getAddLogs() {
    return addLogs;
  }

  public void setAddLogs(AddLog[] addLogs) {
    this.addLogs = addLogs;
  }

  public AddLog getAddLog() {
    return addLog;
  }

  /**
   * 存储数据成功就加1
   */
  public void success(int i) {
    count.addAndGet(i);
    countDownLatch.countDown();
  }

  public void success() {
    count.addAndGet(1);
    countDownLatch.countDown();
  }

  /**
   * 失败不加
   */
  public void failed() {
    countDownLatch.countDown();
  }

  public boolean isCommitLogIndexFlag() {
    return commitLogIndexFlag;
  }

  public void setCommitLogIndexFlag() {
    this.commitLogIndexFlag = true;
  }
}
