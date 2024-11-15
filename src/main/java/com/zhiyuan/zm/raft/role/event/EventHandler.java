package com.zhiyuan.zm.raft.role.event;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zhiyuan.zm.raft.dto.LogEntries;
import com.zhiyuan.zm.raft.role.transaction.TransactionService;
import com.zhiyuan.zm.raft.service.RaftStatus;

/**
 * @author zhouzhiyuan
 * @date 2024/11/15 16:35
 */
public class EventHandler  {

  private static final Logger LOGGER = LoggerFactory.getLogger(EventHandler.class);

  private BlockingQueue<Event> logQueue;

  private ScheduledExecutorService executorService;

  private RaftStatus raftStatus;

  private boolean stopFlag = true;

  public EventHandler(BlockingQueue<Event> logQueue, ScheduledExecutorService executorService, RaftStatus raftStatus) {
    this.logQueue = logQueue;
    this.executorService = executorService;
    this.raftStatus = raftStatus;
  }


  public void run(){
    while (stopFlag) {
      try {
        Event take = logQueue.take();



      } catch (Exception e) {
        LOGGER.error("事件处理任务异常："+e);
      }
    }
  }

}
