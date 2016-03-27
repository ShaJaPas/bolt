package io.lyracommunity.bolt.helper;

import java.security.MessageDigest;
import java.util.Random;

/**
 * Created by keen on 24/03/16.
 */
public class TestData {

    // Get an array filled with random data
    public static byte[] getRandomData(final int size) {
        byte[] data = new byte[size];
        new Random().nextBytes(data);
        return data;
    }

    // Get an array filled with seeded random data
    public static byte[] getRandomData(final int seed, final int size) {
        byte[] data = new byte[size];
        new Random(seed).nextBytes(data);
        return data;
    }

    // Compute the md5 hash
    public static String computeMD5(byte[]... datablocks) throws Exception {
        MessageDigest d = MessageDigest.getInstance("MD5");
        for (byte[] data : datablocks) {
            d.update(data);
        }
        return hexString(d);
    }

    public static String hexString(MessageDigest digest) {
        byte[] messageDigest = digest.digest();
        StringBuilder hexString = new StringBuilder();
        for (byte aMessageDigest : messageDigest) {
            String hex = Integer.toHexString(0xFF & aMessageDigest);
            if (hex.length() == 1) hexString.append('0');
            hexString.append(hex);
        }
        return hexString.toString();
    }

}
