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
