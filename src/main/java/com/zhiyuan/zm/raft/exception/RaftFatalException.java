package com.zhiyuan.zm.raft.exception;

/**
 * Raft 服务致命异常，导致服务无法继续运行
 * @author zhouzhiyuan
 * @date 2026/03/03
 */
public class RaftFatalException extends RuntimeException {

  private final int errorCode;

  public RaftFatalException(String message) {
    super(message);
    this.errorCode = -1;
  }

  public RaftFatalException(String message, int errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public RaftFatalException(String message, Throwable cause) {
    super(message, cause);
    this.errorCode = -1;
  }

  public RaftFatalException(String message, Throwable cause, int errorCode) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }
}
