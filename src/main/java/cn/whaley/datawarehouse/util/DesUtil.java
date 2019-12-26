package cn.whaley.datawarehouse.util;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;
import java.security.SecureRandom;

public class DesUtil {

    private static final String ENCODING = "UTF-8";
    private static String PASSWORD = "792a995939bfec34639b9b2e40acd7b0";


    /**
     * DES加密字符串
     *
     * @param inputStr 需加密的字符串
     * @return 加密后的字符串
     */
    public static String encrypt(String inputStr) {
        String result = "";
        try {
            result = byteArr2HexStr(encryptBytes(inputStr.getBytes(ENCODING)));
        } catch (Exception e) {
            e.printStackTrace();
            result = inputStr;
        }
        return result;
    }


    /**
     * DES解密字符串
     *
     * @param inputStr 需解密的字符串
     * @return 解密后的字符串
     */
    public static String decrypt(String inputStr) {
        String result = "";
        try {
            result = new String(decryptBytes(hexStr2ByteArr(inputStr)), ENCODING);
        } catch (Exception e) {
            e.printStackTrace();
            result = inputStr;
        }
        return result;
    }


    /**
     * 获取加解密Cipher对象
     *
     * @param mode 加解密模式
     * @return 加解密Cipher对象
     * @throws Exception
     */
    private static Cipher getDesCipher(int mode) throws Exception {
        SecureRandom random = new SecureRandom();
        DESKeySpec desKey = new DESKeySpec(PASSWORD.getBytes(ENCODING));
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance("DES");
        SecretKey secureKey = keyFactory.generateSecret(desKey);
        Cipher cipher = Cipher.getInstance("DES");
        cipher.init(mode, secureKey, random);
        return cipher;
    }


    /**
     * 加密字节数组
     *
     * @param byteArr 需加密的字节数组
     * @return 加密后的字节数组
     * @throws Exception
     */
    private static byte[] encryptBytes(byte[] byteArr) throws Exception {
        Cipher cipher = getDesCipher(Cipher.ENCRYPT_MODE);
        return cipher.doFinal(byteArr);
    }


    /**
     * 解密字节数组
     *
     * @param byteArr 需解密的字节数组
     * @return 解密后的字节数组
     * @throws Exception
     */
    private static byte[] decryptBytes(byte[] byteArr) throws Exception {
        Cipher cipher = getDesCipher(Cipher.DECRYPT_MODE);
        return cipher.doFinal(byteArr);
    }


    /**
     * 将byte数组转换为表示16进制值的字符串，
     * 如：byte[]{8,18}转换为：0813，
     * 和public static byte[] hexStr2ByteArr(String strIn)
     * 互为可逆的转换过程
     *
     * @param byteArr 需要转换的byte数组
     * @return 转换后的字符串
     */
    private static String byteArr2HexStr(byte[] byteArr) {
        int len = byteArr.length;
        StringBuffer sb = new StringBuffer(len * 2);
        for (int i = 0; i < len; i++) {
            int intTmp = byteArr[i];
            while (intTmp < 0) {
                intTmp = intTmp + 256;
            }
            if (intTmp < 16) {
                sb.append("0");
            }
            sb.append(Integer.toString(intTmp, 16));
        }
        return sb.toString();
    }


    /**
     * 将表示16进制值的字符串转换为byte数组，
     * 和public static String byteArr2HexStr(byte[] arrB)
     * 互为可逆的转换过程
     *
     * @param hexStr 需要转换的字符串
     * @return 转换后的byte数组
     */
    private static byte[] hexStr2ByteArr(String hexStr) throws Exception {
        byte[] byteArr = hexStr.getBytes(ENCODING);
        int len = byteArr.length;
        byte[] arrOut = new byte[len / 2];
        for (int i = 0; i < len; i = i + 2) {
            String strTmp = new String(byteArr, i, 2);
            arrOut[i / 2] = (byte) Integer.parseInt(strTmp, 16);
        }
        return arrOut;
    }


}

