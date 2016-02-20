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

package bolt.util;

import bolt.BoltEndPoint;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;

/**
 * helper methods
 */
public class Util {

    private static final long SYN = 10000;
    private static final double SYN_D = 10000.0;

    /**
     * get the current timer value in microseconds
     *
     * @return
     */
    public static long getCurrentTime() {
        return System.nanoTime() / 1000;
    }

    /**
     * get the SYN time in microseconds. The SYN time is 0.01 seconds = 10000 microseconds
     *
     * @return
     */
    public static long getSYNTime() {
        return 10000;
    }

    public static double getSYNTimeD() {
        return 10000.0;
    }

    /**
     * get the SYN time in seconds. The SYN time is 0.01 seconds = 10000 microseconds
     *
     * @return
     */
    public static double getSYNTimeSeconds() {
        return 0.01;
    }


    /**
     * perform UDP hole punching to the specified client by sending
     * a dummy packet. A local port will be chosen automatically.
     *
     * @param client - client address
     * @return the local port that can now be accessed by the client
     * @throws IOException
     */
    public static void doHolePunch(BoltEndPoint endpoint, InetAddress client, int clientPort) throws IOException {
        DatagramPacket p = new DatagramPacket(new byte[1], 1);
        p.setAddress(client);
        p.setPort(clientPort);
        endpoint.sendRaw(p);
    }

}
