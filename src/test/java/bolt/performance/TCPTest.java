package bolt.performance;

import bolt.helper.PortUtil;
import org.junit.Test;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

/**
 * Send some data over a TCP connection and measure performance.
 */
public class TCPTest {

    private static final int SERVER_PORT = PortUtil.nextServerPort();
    private volatile long total = 0;
    private volatile boolean serverRunning = true;

    @Test
    public void test1() throws Exception {
        runServer();
        // Client socket
        Socket clientSocket = new Socket("localhost", SERVER_PORT);
        OutputStream os = clientSocket.getOutputStream();
        final int num_packets = 100 * 1000;
        int N = num_packets * 1024;
        byte[] data = new byte[N];
        new Random().nextBytes(data);
        long start = System.currentTimeMillis();

        System.out.println("Sending data block of <" + N + "> bytes.");
        os.write(data);
        os.flush();
        os.close();
        while (serverRunning) Thread.sleep(10);
        long end = System.currentTimeMillis();
        System.out.println("Done. Sending " + N / 1024 + " Kbytes took " + (end - start) + " ms");
        System.out.println("Rate " + N / (end - start) + " Kbytes/sec");
        System.out.println("Server received: " + total);
    }

    private void runServer() throws Exception {
        // Server socket
        final ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
        Runnable serverProcess = () -> {
            try {
                Socket s = serverSocket.accept();
                InputStream is = s.getInputStream();
                byte[] buf = new byte[16384];
                while (true) {
                    int c = is.read(buf);
                    if (c < 0) break;
                    total += c;
                }
                serverRunning = false;
            }
            catch (Exception e) {
                e.printStackTrace();
                serverRunning = false;
            }
        };
        Thread t = new Thread(serverProcess);
        t.start();
    }
}
