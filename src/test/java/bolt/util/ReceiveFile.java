//package bolt.util;
//
//import bolt.BoltClient;
//
//import java.io.BufferedOutputStream;
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.OutputStream;
//import java.net.InetAddress;
//import java.text.NumberFormat;
//
///**
// * helper class for receiving a single file via Bolt
// * Intended to be compatible with the C++ version in
// * the Bolt reference implementation
// * <p>
// * main method USAGE:
// * java -cp ... bolt.util.ReceiveFile <server_ip> <server_port> <remote_filename> <local_filename>
// */
//public class ReceiveFile extends Application {
//
//    private final int serverPort;
//    private final String serverHost;
//    private final String remoteFile;
//    private final String localFile;
//    private final NumberFormat format;
//
//    public ReceiveFile(String serverHost, int serverPort, String remoteFile, String localFile) {
//        this.serverHost = serverHost;
//        this.serverPort = serverPort;
//        this.remoteFile = remoteFile;
//        this.localFile = localFile;
//        format = NumberFormat.getNumberInstance();
//        format.setMaximumFractionDigits(3);
//    }
//
//    public static void main(String[] fullArgs) throws Exception {
//        int serverPort = 65321;
//        String serverHost = "localhost";
//        String remoteFile = "";
//        String localFile = "";
//
//        String[] args = parseOptions(fullArgs);
//
//        try {
//            serverHost = args[0];
//            serverPort = Integer.parseInt(args[1]);
//            remoteFile = args[2];
//            localFile = args[3];
//        }
//        catch (Exception ex) {
//            usage();
//            System.exit(1);
//        }
//
//        ReceiveFile rf = new ReceiveFile(serverHost, serverPort, remoteFile, localFile);
//        rf.run();
//    }
//
//    public static void usage() {
//        System.out.println("Usage: java -cp .. bolt.util.ReceiveFile " +
//                "<server_ip> <server_port> <remote_filename> <local_filename> " +
//                "[--verbose] [--localPort=<port>] [--localIP=<ip>]");
//    }
//
//    public void run() {
//        configure();
//        verbose = true;
//        try {
//            InetAddress myHost = localIP != null ? InetAddress.getByName(localIP) : InetAddress.getLocalHost();
//            BoltClient client = localPort != -1 ? new BoltClient(myHost, localPort) : new BoltClient(myHost);
//            client.connect(InetAddress.getByName(serverHost), serverPort);
//
//            System.out.println("[ReceiveFile] Requesting file " + remoteFile);
//            byte[] fName = remoteFile.getBytes();
//
//            //send file name info
//            byte[] nameinfo = new byte[fName.length + 4];
//            System.arraycopy(encode(fName.length), 0, nameinfo, 0, 4);
//            System.arraycopy(fName, 0, nameinfo, 4, fName.length);
//
//            out.write(nameinfo);
//            out.flush();
//            //pause the sender to save some CPU time
//            out.pauseOutput();
//
//            //read size info (an 64 bit number)
//            byte[] sizeInfo = new byte[8];
//
//            int total = 0;
//            while (total < sizeInfo.length) {
//                int r = in.read(sizeInfo);
//                if (r < 0) break;
//                total += r;
//            }
//            long size = decode(sizeInfo, 0);
//
//            File file = new File(localFile);
//            System.out.println("[ReceiveFile] Write to local file <" + file.getAbsolutePath() + ">");
//            FileOutputStream fos = new FileOutputStream(file);
//            OutputStream os = new BufferedOutputStream(fos, 1024 * 1024);
//            try {
//                System.out.println("[ReceiveFile] Reading <" + size + "> bytes.");
//                long start = System.currentTimeMillis();
//                //and read the file data
//                TestUtil.copy(in, os, size, false);
//                long end = System.currentTimeMillis();
//                double rate = 1000.0 * size / 1024 / 1024 / (end - start);
//                System.out.println("[ReceiveFile] Rate: " + format.format(rate) + " MBytes/sec. "
//                        + format.format(8 * rate) + " MBit/sec.");
//
//                client.shutdown();
//
//                if (verbose) System.out.println(client.getStatistics());
//
//            }
//            finally {
//                fos.close();
//            }
//        }
//        catch (Exception ex) {
//            throw new RuntimeException(ex);
//        }
//    }
//
//}
