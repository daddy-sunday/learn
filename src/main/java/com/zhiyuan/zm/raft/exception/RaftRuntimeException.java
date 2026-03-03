package com.zhiyuan.zm.raft.exception;

/**
 * Raft 服务运行时异常
 * @author zhouzhiyuan
 * @date 2026/03/03
 */
public class RaftRuntimeException extends RuntimeException {

  private final int errorCode;

  public RaftRuntimeException(String message) {
    super(message);
    this.errorCode = -1;
  }

  public RaftRuntimeException(String message, int errorCode) {
    super(message);
    this.errorCode = errorCode;
  }

  public RaftRuntimeException(String message, Throwable cause) {
    super(message, cause);
    this.errorCode = -1;
  }

  public RaftRuntimeException(String message, Throwable cause, int errorCode) {
    super(message, cause);
    this.errorCode = errorCode;
  }

  public int getErrorCode() {
    return errorCode;
  }
}
