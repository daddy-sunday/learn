package com.zhiyuan.zm.raft.constant;

/**
 * 数据操作类型常量
 */
public class DataOperationType {
  public static final byte DELETE = 1;
  public static final byte UPDATE = 2;
  public static final byte INSERT = 3;
  public static final byte EMPTY = 4;

  public static final byte TRANSACTION = 5;
  public static final byte CONFIG_CHANGE = 6;

  // ==================== MVCC 相关操作类型 ====================

  /**
   * MVCC 数据写入（创建新版本）
   */
  public static final byte MVCC_WRITE = 7;

  /**
   * MVCC 数据删除（标记删除版本）
   */
  public static final byte MVCC_DELETE = 8;

  /**
   * MVCC 事务提交
   */
  public static final byte MVCC_COMMIT = 9;

  /**
   * MVCC 事务回滚
   */
  public static final byte MVCC_ROLLBACK = 10;

  /**
   * MVCC 版本清理（GC）
   */
  public static final byte MVCC_GC = 11;

  /**
   * MVCC 数据写入（事务内 Put 操作）
   */
  public static final byte MVCC_PUT = 12;
}
