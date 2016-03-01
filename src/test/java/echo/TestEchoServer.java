package echo;

import junit.framework.Assert;
import org.junit.Test;
import bolt.BoltClient;
import bolt.util.TestUtil;

import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.util.concurrent.CompletableFuture;

public class TestEchoServer {

    @Test
    public void test1() throws Exception {
        EchoServer es = new EchoServer(65321);
        CompletableFuture.runAsync(es);
        Thread.sleep(1000);
        BoltClient client = new BoltClient(InetAddress.getByName("localhost"), 12345);
        client.connect("localhost", 65321);
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
