/*********************************************************************************
 * Copyright (c) 2010 Forschungszentrum Juelich GmbH
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * (1) Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the disclaimer at the end. Redistributions in
 * binary form must reproduce the above copyright notice, this list of
 * conditions and the following disclaimer in the documentation and/or other
 * materials provided with the distribution.
 * <p>
 * (2) Neither the name of Forschungszentrum Juelich GmbH nor the names of its
 * contributors may be used to endorse or promote products derived from this
 * software without specific prior written permission.
 * <p>
 * DISCLAIMER
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 *********************************************************************************/

package bolt;

import bolt.util.ReceiveBuffer;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The BoltInputStream receives data blocks from the {@link BoltSocket}
 * as they become available, and places them into an ordered,
 * bounded queue (the flow window) for reading by the application
 */
public class BoltInputStream extends InputStream {

    //the socket owning this inputstream
    private final BoltSocket socket;

    private final ReceiveBuffer receiveBuffer;

    //set to 'false' by the receiver when it gets a shutdown signal from the peer
    //see the noMoreData() method
    private final AtomicBoolean expectMoreData = new AtomicBoolean(true);
    private final byte[] single = new byte[1];
    //offset into currentChunk
    int offset = 0;
    private volatile boolean closed = false;
    private volatile boolean blocking = true;
    private AppData currentChunk = null;

    /**
     * create a new {@link BoltInputStream} connected to the given socket
     *
     * @param socket - the {@link BoltSocket}
     * @throws IOException
     */
    public BoltInputStream(BoltSocket socket) throws IOException {
        this.socket = socket;
        int capacity = socket != null ? 2 * socket.getSession().getFlowWindowSize() : 128;
        long initialSequenceNum = socket != null ? socket.getSession().getInitialSequenceNumber() : 1;
        receiveBuffer = new ReceiveBuffer(capacity, initialSequenceNum);
    }

    @Override
    public int read() throws IOException {
        int b = 0;
        while (b == 0) {
            b = read(single);
            if (b == 0) {
                try {
                    while (receiveBuffer.isEmpty()) {
                        Thread.sleep(20);
                    }
                } catch (InterruptedException ie) {
                    throw new IOException(ie);
                }
            }
        }

        if (b > 0) {
            return 0xFF & single[0];
        } else {
            return b;
        }
    }

    @Override
    public int read(byte[] target) throws IOException {
        try {
            int read = 0;
            updateCurrentChunk(false);
            while (currentChunk != null) {
                byte[] data = currentChunk.data;
                int length = Math.min(target.length - read, data.length - offset);
                System.arraycopy(data, offset, target, read, length);
                read += length;
                offset += length;
                //check if chunk has been fully read
                if (offset >= data.length) {
                    currentChunk = null;
                    offset = 0;
                }

                //if no more space left in target, exit now
                if (read == target.length) {
                    return read;
                }

                updateCurrentChunk(blocking && read == 0);
            }

            if (read > 0) return read;
            if (closed) return -1;
            if (expectMoreData.get() || !receiveBuffer.isEmpty()) return 0;
            //no more data
            return -1;

        } catch (Exception ex) {
            IOException e = new IOException();
            e.initCause(ex);
            throw e;
        }
    }

    /**
     * Reads the next valid chunk of application data from the queue<br/>
     * <p>
     * In blocking mode,this method will block until data is available or the socket is closed,
     * otherwise it will wait for at most 10 milliseconds.
     *
     * @throws InterruptedException
     */
    private void updateCurrentChunk(boolean block) throws IOException {
        if (currentChunk != null) return;

        while (true) {
            try {
                if (block) {
                    currentChunk = receiveBuffer.poll(1, TimeUnit.MILLISECONDS);
                    while (!closed && currentChunk == null) {
                        currentChunk = receiveBuffer.poll(1000, TimeUnit.MILLISECONDS);
                    }
                } else currentChunk = receiveBuffer.poll(10, TimeUnit.MILLISECONDS);

            } catch (InterruptedException ie) {
                throw new IOException(ie);
            }
            return;
        }
    }

    /**
     * new application data
     *
     * @param data
     */
    protected boolean haveNewData(long sequenceNumber, byte[] data) throws IOException {
        return receiveBuffer.offer(new AppData(sequenceNumber, data));
    }

    @Override
    public void close() throws IOException {
        if (closed) return;
        closed = true;
        noMoreData();
    }

    public BoltSocket getSocket() {
        return socket;
    }

    /**
     * sets the blocking mode
     *
     * @param block
     */
    public void setBlocking(boolean block) {
        this.blocking = block;
    }

    public int getReceiveBufferSize() {
        return receiveBuffer.getSize();
    }

    /**
     * notify the input stream that there is no more data
     *
     * @throws IOException
     */
    protected void noMoreData() throws IOException {
        expectMoreData.set(false);
    }

    /**
     * used for storing application data and the associated
     * sequence number in the queue in ascending order
     */
    public static class AppData implements Comparable<AppData> {
        final long sequenceNumber;
        final byte[] data;

        public AppData(long sequenceNumber, byte[] data) {
            this.sequenceNumber = sequenceNumber;
            this.data = data;
        }

        public int compareTo(AppData o) {
            return (int) (sequenceNumber - o.sequenceNumber);
        }

        public String toString() {
            return sequenceNumber + "[" + data.length + "]";
        }

        public long getSequenceNumber() {
            return sequenceNumber;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result
                    + (int) (sequenceNumber ^ (sequenceNumber >>> 32));
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            AppData other = (AppData) obj;
            if (sequenceNumber != other.sequenceNumber)
                return false;
            return true;
        }


    }

}
