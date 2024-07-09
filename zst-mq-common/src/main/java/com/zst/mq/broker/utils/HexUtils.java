package com.zst.mq.broker.utils;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;

public class HexUtils {
    public static String toHexString(String str) {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02x", b));
        }
        return sb.toString();
    }

    public static String hex2String(String hexString) {
        try {
            String result = "";
            char[] charValues = hexString.toCharArray();
            byte[] b = new byte[charValues.length / 2];
            for (int i = 0; i <= (charValues.length - 1) / 2; i++) {
                String c = String.valueOf(charValues[i * 2]) + String.valueOf(charValues[i * 2 + 1]);
                int high = Integer.parseInt(c.substring(0, 1), 16);
                int low = Integer.parseInt(c.substring(1), 16);
                b[i] = (byte) (high * 16 + low);
            }
            try {
                result = new String(b, StandardCharsets.UTF_8.name());
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                return hexString;
            }
            return result;
        } catch (Throwable e) {
            return hexString;
        }
    }
}
