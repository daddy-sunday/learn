package org.example;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.Pipe;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.concurrent.CompletableFuture;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 *@author zhouzhiyuan
 *@date 2022/5/30
 */
public class JavaStreamOptimize {

  private static final String FILE_NAME = "aaa";

  private static final String SUFFIX_FILE = "01";

  private static final long FILE_SIZE = 01;

  private static final String JPG_FILE_PATH = "/tmp/jpg/";

  private static final String JPG_FILE = "";

  private static String ZIP_FILE = "/tmp";


  public static void zipFileBuffer() {
    File zipFile = new File(ZIP_FILE);
    try (ZipOutputStream zipOut = new ZipOutputStream(
        new FileOutputStream(zipFile)); BufferedOutputStream bufferedOutputStream = new BufferedOutputStream(
        zipOut)) {         //开始时间
      long beginTime = System.currentTimeMillis();
      for (int i = 0; i < 10; i++) {
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(new File(JPG_FILE)))) {
          zipOut.putNextEntry(new ZipEntry(FILE_NAME + i));
          int temp = 0;
          while ((temp = bufferedInputStream.read()) != -1) {
            bufferedOutputStream.write(temp);
          }
        }
      }
      printInfo(beginTime);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  //Version 5 使用Pip
  public static void zipFilePip() {
    long beginTime = System.currentTimeMillis();
    try (WritableByteChannel out = Channels
        .newChannel(new FileOutputStream(ZIP_FILE))) {
      Pipe pipe = Pipe
          .open();
      //异步任务
      CompletableFuture.runAsync(() -> runTask(pipe));
      // 获取读通道
      ReadableByteChannel readableByteChannel = pipe.source();
      ByteBuffer buffer = ByteBuffer.allocate(((int) FILE_SIZE) * 10);
      while (readableByteChannel.read(buffer) >= 0) {
        buffer.flip();
        out.write(buffer);
        buffer.clear();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    printInfo(beginTime);
  }

  private static void printInfo(long time){
    System.out.println("  "+time);
  }

  //异步任务
  public static void runTask(Pipe pipe) {
    try (ZipOutputStream zos = new ZipOutputStream(
        Channels.newOutputStream(pipe.sink())); WritableByteChannel out = Channels.newChannel(zos)) {
      System.out.println("Begin");
      for (int i = 0; i < 10; i++) {
        zos.putNextEntry(new ZipEntry(i + SUFFIX_FILE));
        FileChannel jpgChannel = new FileInputStream(new File(JPG_FILE_PATH)).getChannel();
        jpgChannel.transferTo(0,FILE_SIZE, out);
        jpgChannel.close();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void zipFileChannel() {
    //开始时间
    long beginTime = System.currentTimeMillis();
    File zipFile = new File(ZIP_FILE);
    try (ZipOutputStream zipOut = new ZipOutputStream(
        new FileOutputStream(zipFile)); WritableByteChannel writableByteChannel = Channels.newChannel(zipOut)) {
      for (int i = 0; i < 10; i++) {
        try (FileChannel fileChannel = new FileInputStream(JPG_FILE).getChannel()) {
          zipOut.putNextEntry(new ZipEntry(i + SUFFIX_FILE));
          fileChannel.transferTo(0, FILE_SIZE, writableByteChannel);
        }
      }
      printInfo(beginTime);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  //Version 4 使用Map映射文件
  public static void zipFileMap() {
    //开始时间
    long beginTime = System.currentTimeMillis();
    File zipFile = new File(ZIP_FILE);
    try (ZipOutputStream zipOut = new ZipOutputStream(
        new FileOutputStream(zipFile)); WritableByteChannel writableByteChannel = Channels.newChannel(zipOut)) {
      for (int i = 0; i < 10; i++) {
        zipOut.putNextEntry(new ZipEntry(i
            + SUFFIX_FILE));
        //内存中的映射文件
        MappedByteBuffer mappedByteBuffer = new RandomAccessFile(JPG_FILE_PATH, "r").getChannel()
            .map(FileChannel.MapMode.READ_ONLY, 0, FILE_SIZE);
        writableByteChannel.write(mappedByteBuffer);
      }
      printInfo(beginTime);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
