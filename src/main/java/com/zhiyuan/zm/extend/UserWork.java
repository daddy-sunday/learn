package com.zhiyuan.zm.extend;

/**
 *
 * 实现该接口并在启动时设置实现类，在leader选举成功时会执行run方法中的代码，在leader让位时执行stop方法
 *
 * @author zhouzhiyuan
 * @date 2023/2/1 16:00
 */
public interface UserWork extends Runnable {

  /**
   * 停止用户业务逻辑
   */
  void stop();
}
