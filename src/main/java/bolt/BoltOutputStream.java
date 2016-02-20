package bolt;

import java.io.IOException;
import java.io.OutputStream;

/**
 * BoltOutputStream provides a Bolt version of {@link OutputStream}
 */
public class BoltOutputStream extends OutputStream {

    private final BoltSocket socket;

    private volatile boolean closed;

    public BoltOutputStream(BoltSocket socket) {
        this.socket = socket;
    }

    @Override
    public void write(int args) throws IOException {
        checkClosed();
        socket.doWrite(new byte[]{(byte) args});
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        checkClosed();
        socket.doWrite(b, off, len);
    }

    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    @Override
    public void flush() throws IOException {
        try {
            checkClosed();
            socket.flush();
        } catch (InterruptedException ie) {
            IOException io = new IOException();
            io.initCause(ie);
            throw io;
        }
    }

    /**
     * This method signals the Bolt sender that it can pause the
     * sending thread. The Bolt sender will resume when the next
     * write() call is executed.<br/>
     * For example, one can use this method on the receiving end
     * of a file transfer, to save some CPU time which would otherwise
     * be consumed by the sender thread.
     */
    public void pauseOutput() throws IOException {
        socket.getSender().pause();
    }


    /**
     * close this output stream
     */
    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
    }

    private void checkClosed() throws IOException {
        if (closed) throw new IOException("Stream has been closed");
    }
}
