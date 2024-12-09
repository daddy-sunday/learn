package com.zhiyuan.zm.raft.dto;

import java.nio.charset.StandardCharsets;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializeConfig;
import com.zhiyuan.zm.raft.util.ByteUtil;

import cn.hutool.json.JSONUtil;

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

  public Row(Long key, Long value) {
    this.key = ByteUtil.longToBytes(key);
    this.value = ByteUtil.longToBytes(value);
  }

  public Row(byte[] key, Object value) {
    this.key = key;
    this.value = JSON.toJSONBytes(value);
  }

  public Row(byte[] key, Long value) {
    this.key = key;
    this.value = ByteUtil.longToBytes(value);
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
