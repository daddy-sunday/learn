package org.example;

import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.PublicKey;

import javax.crypto.Cipher;

import org.junit.Test;
import org.springframework.core.io.ClassPathResource;

/**
 *@author zhouzhiyuan
 *@date 2022/3/23
 */
public class PasswordTest {


  public void testEncrypt() {

  }

  private PublicKey publicKey;

  private PrivateKey privateKey;

  {
    try {
      char[] password = "yunli@AUG".toCharArray();
      KeyStore keyStore = KeyStore.getInstance("PKCS12");
      ClassPathResource resource = new ClassPathResource("bigdata.pfx");
      keyStore.load(resource.getInputStream(), password);
      String aliase = keyStore.aliases().nextElement();
      publicKey = keyStore.getCertificate(aliase).getPublicKey();
      privateKey = (PrivateKey) keyStore.getKey(aliase, password);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  @Test
  public void test01() throws Exception {

    String a = "哈哈";

    byte[] bytes1 = encryptByPublicKey(a.getBytes());

    byte[] bytes = decryptByPrivateKey(bytes1);

    System.out.println(new String(bytes));

  }

  /**
   * 通过私钥解密
   * @param encryptedData
   * @return
   * @throws Exception
   */
  public byte[] decryptByPrivateKey(byte[] encryptedData) throws Exception {
    Cipher cipher = Cipher.getInstance("RSA");
    cipher.init(Cipher.DECRYPT_MODE, privateKey);
    return cipher.doFinal(encryptedData);
  }

  /**
   * 通过公钥加密
   * @param data
   * @return
   * @throws Exception
   */
  public byte[] encryptByPublicKey(byte[] data) throws Exception {
    Cipher cipher = Cipher.getInstance("RSA");
    cipher.init(Cipher.ENCRYPT_MODE, publicKey);
    return cipher.doFinal(data);
  }
}
