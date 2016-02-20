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

import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;


public class BoltServerSocket {

    private static final Logger logger = Logger.getLogger(BoltClient.class.getName());

    private final BoltEndPoint endpoint;

    private boolean started = false;

    private volatile boolean shutdown = false;

    /**
     * create a Bolt ServerSocket
     *
     * @param localAddress
     * @param port         - the local port. If 0, an ephemeral port will be chosen
     */
    public BoltServerSocket(InetAddress localAddress, int port) throws SocketException, UnknownHostException {
        endpoint = new BoltEndPoint(localAddress, port);
        logger.info("Created server endpoint on port " + endpoint.getLocalPort());
    }

    //starts a server on localhost
    public BoltServerSocket(int port) throws SocketException, UnknownHostException {
        this(InetAddress.getLocalHost(), port);
    }

    /**
     * listens and blocks until a new client connects and returns a valid {@link BoltSocket}
     * for the new connection
     *
     * @return
     */
    public synchronized BoltSocket accept() throws InterruptedException {
        if (!started) {
            endpoint.start(true);
            started = true;
        }
        while (!shutdown) {
            BoltSession session = endpoint.accept(10000, TimeUnit.MILLISECONDS);
            if (session != null) {
                //wait for handshake to complete
                while (!session.isReady() || session.getSocket() == null) {
                    Thread.sleep(100);
                }
                return session.getSocket();
            }
        }
        throw new InterruptedException();
    }

    public void shutDown() {
        shutdown = true;
        endpoint.stop();
    }

    public BoltEndPoint getEndpoint() {
        return endpoint;
    }
}
