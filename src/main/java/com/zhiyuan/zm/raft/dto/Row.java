package com.zhiyuan.zm.raft.dto;

/**
 *@author zhouzhiyuan
 *@date 2022/4/2
 */
public class Row {

  private byte[] key;

  private byte[] value;

  public Row(byte[] key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public byte[] getKey() {
    return key;
  }

  public void setKey(byte[] key) {
    this.key = key;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }
}
