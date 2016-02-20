package bolt.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.MessageDigest;

/**
 * Created by keen on 20/02/16.
 */
public class TestUtil {


    /**
     * copy input data from the source stream to the target stream
     *
     * @param source - input stream to read from
     * @param target - output stream to write to
     * @throws IOException
     */

    public static void copy(InputStream source, OutputStream target) throws Exception {
        copy(source, target, -1, false);
    }

    /**
     * copy input data from the source stream to the target stream
     *
     * @param source - input stream to read from
     * @param target - output stream to write to
     * @param size   - how many bytes to copy (-1 for no limit)
     * @param flush  - whether to flush after each write
     * @throws IOException
     */
    public static void copy(InputStream source, OutputStream target, long size, boolean flush) throws IOException {
        byte[] buf = new byte[8 * 65536];
        int c;
        long read = 0;
        while (true) {
            c = source.read(buf);
            if (c < 0) break;
            read += c;
            target.write(buf, 0, c);
            if (flush) target.flush();
            if (read >= size && size > -1) break;
        }
        if (!flush) target.flush();
    }

    /**
     * read a line terminated by a new line '\n' character
     *
     * @param input - the input string to read from
     * @return the line read or <code>null</code> if end of input is reached
     * @throws IOException
     */
    public static String readLine(InputStream input) throws IOException {
        char term = System.getProperty("line.separator").charAt(0);
        return readLine(input, term);
    }

    /**
     * read a line from the given input stream
     *
     * @param input          - the input stream
     * @param terminatorChar - the character used to terminate lines
     * @return the line read or <code>null</code> if end of input is reached
     * @throws IOException
     */
    public static String readLine(InputStream input, char terminatorChar) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while (true) {
            int c = input.read();
            if (c < 0 && bos.size() == 0) return null;
            if (c < 0 || c == terminatorChar) break;
            else bos.write(c);
        }
        return bos.size() > 0 ? bos.toString() : null;
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
