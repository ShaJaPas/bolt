package bolt;

import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

/**
 * BoltSocket is analogous to a normal java.net.Socket, it provides input and
 * output streams for the application.
 */
public class BoltSocket {

    //endpoint
    private final BoltEndPoint endpoint;
    private final BoltSession session;
    private volatile boolean active;
    //processing received data
    private final BoltReceiver receiver;
    private final BoltSender sender;
    private BoltInputStream inputStream;
    private BoltOutputStream outputStream;

    /**
     * @param endpoint
     * @param session
     * @throws SocketException,UnknownHostException
     */
    public BoltSocket(final BoltEndPoint endpoint, final BoltSession session) throws SocketException, UnknownHostException {
        this.endpoint = endpoint;
        this.session = session;
        this.receiver = new BoltReceiver(session, endpoint);
        this.sender = new BoltSender(session, endpoint);
    }

    public BoltReceiver getReceiver() {
        return receiver;
    }

    public BoltSender getSender() {
        return sender;
    }

    public boolean isActive() {
        return active;
    }

    /**
     * get the input stream for reading from this socket
     *
     * @return
     */
    public synchronized BoltInputStream getInputStream() throws IOException {
        if (inputStream == null) {
            inputStream = new BoltInputStream(this);
        }
        return inputStream;
    }

    /**
     * get the output stream for writing to this socket
     *
     * @return
     */
    public synchronized BoltOutputStream getOutputStream() {
        if (outputStream == null) {
            outputStream = new BoltOutputStream(this);
        }
        return outputStream;
    }

    public final BoltSession getSession() {
        return session;
    }

    /**
     * write single block of data without waiting for any acknowledgement
     *
     * @param data
     */
    protected void doWrite(byte[] data) throws IOException {
        doWrite(data, 0, data.length);

    }

    /**
     * write the given data
     *
     * @param data   - the data array
     * @param offset - the offset into the array
     * @param length - the number of bytes to write
     * @throws IOException
     */
    protected void doWrite(byte[] data, int offset, int length) throws IOException {
        try {
            doWrite(data, offset, length, 10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            throw new IOException(ie);
        }
    }

    /**
     * write the given data, waiting at most for the specified time if the queue is full
     *
     * @param data
     * @param offset
     * @param length
     * @param timeout
     * @param units
     * @throws IOException          - if data cannot be sent
     * @throws InterruptedException
     */
    protected void doWrite(byte[] data, int offset, int length, int timeout, TimeUnit units) throws IOException, InterruptedException {
        ByteBuffer bb = ByteBuffer.wrap(data, offset, length);
        while (bb.remaining() > 0) {
            try {
                sender.sendPacket(bb, timeout, units);
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }
        if (length > 0) active = true;
    }

    /**
     * Will block until the outstanding packets have really been sent out
     * and acknowledged.
     */
    protected void flush() throws InterruptedException, IllegalStateException {
        if (!active) return;
        final long seqNo = sender.getCurrentSequenceNumber();
        if (seqNo < 0) throw new IllegalStateException();
        while (active && !sender.isSentOut(seqNo)) {
            Thread.sleep(5);
        }
        if (seqNo > -1) {
            // Wait until data has been sent out and acknowledged.
            while (active && !sender.haveAcknowledgementFor(seqNo)) {
                sender.waitForAck(seqNo);
            }
        }
        //TODO need to check if we can pause the sender...
//        sender.pause();
    }

    /**
     * Writes and wait for ack.
     */
    protected void doWriteBlocking(byte[] data) throws IOException, InterruptedException {
        doWrite(data);
        flush();
    }

    /**
     * Close the connection.
     *
     * @throws IOException
     */
    public void close() throws IOException {
        if (inputStream != null) inputStream.close();
        if (outputStream != null) outputStream.close();
        active = false;
    }

}
