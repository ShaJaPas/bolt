//package bolt;
//
//import bolt.util.ReceiveFile;
//import bolt.util.SendFile;
//import org.junit.Test;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.util.concurrent.CompletableFuture;
//
//import static org.junit.Assert.assertEquals;
//
//public class TestSendFileReceiveFile extends BoltTestBase {
//
//    volatile boolean serverStarted = false;
//
//    public static void main(String[] args) throws Exception {
//        new TestSendFileReceiveFile().test1();
//    }
//
//    @Test
//    public void test1() throws Exception {
//        runServer();
//        do {
//            Thread.sleep(500);
//        } while (!serverStarted);
//
//        File f = new File("src/test/java/datafile");
//        //File f=new File("/tmp/100MB");
//
//        File tmp = File.createTempFile("boltest-", null);
//
//        String[] args = new String[]{"localhost", "65321", f.getAbsolutePath(), tmp.getAbsolutePath()};
//        ReceiveFile.main(args);
//        //check temp data file
//        String md5_sent = readAll(new FileInputStream(f), 4096);
//        String md5_received = readAll(new FileInputStream(tmp), 4096);
//        assertEquals(md5_sent, md5_received);
//    }
//
//    private void runServer() {
//        CompletableFuture.runAsync(() -> {
//            try {
//                serverStarted = true;
//                SendFile.main(new String[]{"65321"});
//            } catch (Exception ex) {
//                ex.printStackTrace();
//            }
//        });
//    }
//}
