package echo;

import junit.framework.Assert;
import org.junit.Test;
import bolt.BoltClient;
import bolt.util.TestUtil;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;

public class TestEchoServerMultiClient {

    @Test
    public void testTwoClients() throws Exception {
        EchoServer es = new EchoServer(65321);
        es.start();
        Thread.sleep(1000);

        BoltClient client = new BoltClient(InetAddress.getByName("localhost"), 12345);
        client.connect("localhost", 65321);
        doClientCommunication(client);

        BoltClient client2 = new BoltClient(InetAddress.getByName("localhost"), 12346);
        client2.connect("localhost", 65321);
        doClientCommunication(client2);

        es.stop();
    }

    private void doClientCommunication(BoltClient client) throws Exception {
        PrintWriter pw = new PrintWriter(new OutputStreamWriter(client.getOutputStream(), "UTF-8"));
        pw.println("test");
        pw.flush();
        System.out.println("Message sent.");
        client.getInputStream().setBlocking(false);
        String line = TestUtil.readLine(client.getInputStream());
        Assert.assertNotNull(line);
        System.out.println(line);
        Assert.assertEquals("test", line);
    }
}
