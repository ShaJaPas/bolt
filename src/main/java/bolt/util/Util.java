package bolt.util;

import bolt.BoltEndPoint;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;

/**
 * helper methods
 */
public class Util {

    private static final long SYN = 10000;
    private static final double SYN_D = 10000.0;

    /**
     * get the current timer value in microseconds
     *
     * @return
     */
    public static long getCurrentTime() {
        return System.nanoTime() / 1000;
    }

    /**
     * get the SYN time in microseconds. The SYN time is 0.01 seconds = 10000 microseconds
     *
     * @return
     */
    public static long getSYNTime() {
        return 10000;
    }

    public static double getSYNTimeD() {
        return 10000.0;
    }

    /**
     * get the SYN time in seconds. The SYN time is 0.01 seconds = 10000 microseconds
     *
     * @return
     */
    public static double getSYNTimeSeconds() {
        return 0.01;
    }


    /**
     * perform UDP hole punching to the specified client by sending
     * a dummy packet. A local port will be chosen automatically.
     *
     * @param client - client address
     * @return the local port that can now be accessed by the client
     * @throws IOException
     */
    public static void doHolePunch(BoltEndPoint endpoint, InetAddress client, int clientPort) throws IOException {
        DatagramPacket p = new DatagramPacket(new byte[1], 1);
        p.setAddress(client);
        p.setPort(clientPort);
        endpoint.sendRaw(p);
    }

}
