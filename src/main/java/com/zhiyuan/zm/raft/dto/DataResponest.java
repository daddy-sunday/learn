package com.zhiyuan.zm.raft.dto;

import java.io.Serializable;

import com.zhiyuan.zm.raft.constant.StatusCode;

/**
 * 数据响应类
 * @author zhouzhiyuan
 * @date 2021/11/23
 * @deprecated 请使用 {@link DataResponse}
 */
@Deprecated
public class DataResponest extends DataResponse {

  public DataResponest() {
    super();
  }

  public DataResponest(int status) {
    super(status);
  }

  public DataResponest(String message) {
    super(message);
  }

  public DataResponest(int status, String message) {
    super(status, message);
  }
}
