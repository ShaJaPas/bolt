package echo;

import bolt.BoltInputStream;
import bolt.BoltOutputStream;
import bolt.BoltServerSocket;
import bolt.BoltSocket;

import java.io.*;
import java.net.InetAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class EchoServer implements Runnable {

    final ExecutorService pool = Executors.newFixedThreadPool(2);

    final BoltServerSocket server;

    volatile boolean started = false;
    volatile boolean stopped = false;

    public EchoServer(int port) throws Exception {
        server = new BoltServerSocket(InetAddress.getByName("localhost"), port);
    }

    public void stop() {
        stopped = true;
    }

    public void run() {
        try {
            started = true;
            while (!stopped) {
                final BoltSocket socket = server.accept();
                pool.execute(new Request(socket));
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    static String readLine(InputStream r) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        while (true) {
            int c = r.read();
            if (c < 0 && bos.size() == 0) return null;
            if (c < 0 || c == 10) break;
            else bos.write(c);
        }
        return bos.toString();
    }


    public static class Request implements Runnable {

        final BoltSocket socket;

        public Request(BoltSocket socket) {
            this.socket = socket;
        }

        public void run() {
            try {
                System.out.println("Processing request from <" + socket.getSession().getDestination() + ">");
                BoltInputStream in = socket.getInputStream();
                BoltOutputStream out = socket.getOutputStream();
                PrintWriter writer = new PrintWriter(new OutputStreamWriter(out));
                String line = readLine(in);
                if (line != null) {
                    System.out.println("ECHO: " + line);
                    //echo back the line
                    writer.println(line);
                    writer.flush();
                }
                System.out.println("Request from <" + socket.getSession().getDestination() + "> finished.");
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
    }

}
