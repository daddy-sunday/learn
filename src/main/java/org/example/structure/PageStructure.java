package org.example.structure;

/**
 *@author zhouzhiyuan
 *@date 2021/9/17
 */
public class PageStructure {
  //页类型
  private Byte type = 1;
  private Long[] data ;

  public PageStructure() {
    data = new Long[2047];
  }

  public PageStructure(Long[] data) {
    this.data = data;
  }

  public PageStructure(Byte type, Long[] data) {
    this.type = type;
    this.data = data;
  }
}
