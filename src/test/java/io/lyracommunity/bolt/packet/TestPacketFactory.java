package io.lyracommunity.bolt.packet;

import io.lyracommunity.bolt.BoltPacket;
import io.lyracommunity.bolt.util.SeqNum;
import org.junit.Test;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.*;

public class TestPacketFactory {

    private static void print(byte[] arr) {
        System.out.print("[");
        for (byte b : arr) {
            System.out.print(" " + (b & 0xFF));
        }
        System.out.println(" ]");
    }

    @Test
    public void testData() throws IOException {
        String test = "sdjfsdjfldskjflds";

        byte[] data = test.getBytes();
        data[0] = (byte) (data[0] & 0x0F);
        BoltPacket p = PacketFactory.createPacket(data);
        DataPacket recv = (DataPacket) p;

        assertTrue(p instanceof DataPacket);
        assertArrayEquals(data, recv.getEncoded());
    }

    @Test
    public void testConnectionHandshake() throws IOException {
        ConnectionHandshake p1 = new ConnectionHandshake();
        p1.setDestinationID(1);

        p1.setConnectionType(1);
        p1.setSocketType(1);
        p1.setInitialSeqNo(321);
        p1.setPacketSize(128);
        p1.setMaxFlowWndSize(128);
        p1.setSocketID(1);
        p1.setBoltVersion(1);
        p1.setAddress(InetAddress.getLocalHost());
        p1.setCookie(SeqNum.randomInt());

        byte[] p1_data = p1.getEncoded();

        BoltPacket p = PacketFactory.createPacket(p1_data);
        ConnectionHandshake p2 = (ConnectionHandshake) p;
        assertEquals(p1, p2);

    }

    @Test
    public void testAcknowledgement() throws IOException {
        Acknowledgement p1 = new Acknowledgement();
        p1.setAckSequenceNumber(1234);
        p1.setDestinationID(1);
        p1.setBufferSize(128);
        p1.setEstimatedLinkCapacity(16);
        p1.setAckNumber(9870);
        p1.setPacketReceiveRate(1000);
        p1.setRoundTripTime(1000);
        p1.setRoundTripTimeVar(500);

        byte[] p1_data = p1.getEncoded();
        BoltPacket p = PacketFactory.createPacket(p1_data);
        Acknowledgement p2 = (Acknowledgement) p;
        assertEquals(p1, p2);
    }

    @Test
    public void testAcknowledgementOfAcknowledgement() throws IOException {
        Acknowledgment2 p1 = new Acknowledgment2();
        p1.setAckSequenceNumber(1230);
        p1.setDestinationID(1);

        byte[] p1_data = p1.getEncoded();
        BoltPacket p = PacketFactory.createPacket(p1_data);
        Acknowledgment2 p2 = (Acknowledgment2) p;
        assertEquals(p1, p2);


    }

    @Test
    public void testNegativeAcknowledgement() throws IOException {
        NegativeAcknowledgement p1 = new NegativeAcknowledgement();
        p1.setDestinationID(2);
        p1.addLossInfo(5);
        p1.addLossInfo(6);
        p1.addLossInfo(7, 10);
        byte[] p1_data = p1.getEncoded();

        BoltPacket p = PacketFactory.createPacket(p1_data);
        NegativeAcknowledgement p2 = (NegativeAcknowledgement) p;
        assertEquals(p1, p2);

        assertEquals((Integer) 5, p2.getDecodedLossInfo().get(0));
        assertEquals(6, p2.getDecodedLossInfo().size());
    }

    @Test
    public void testNegativeAcknowledgement2() throws IOException {
        final NegativeAcknowledgement p1 = new NegativeAcknowledgement();
        p1.setDestinationID(2);
        final List<Integer> loss = IntStream.of(5, 6, 7, 8, 9, 11).boxed().collect(Collectors.toList());

        p1.addLossInfo(loss);
        final byte[] encoded = p1.getEncoded();

        final NegativeAcknowledgement p2 = (NegativeAcknowledgement) PacketFactory.createPacket(encoded);
        assertEquals(p1, p2);

        assertEquals((Integer) 5, p2.getDecodedLossInfo().get(0));
        assertEquals(6, p2.getDecodedLossInfo().size());
    }

    @Test
    public void testNegativeAcknowledgement3() throws IOException {
        NegativeAcknowledgement p1 = new NegativeAcknowledgement();
        p1.setDestinationID(2);
        p1.addLossInfo(5);
        p1.addLossInfo(6);
        p1.addLossInfo(147, 226);
        byte[] p1_data = p1.getEncoded();

        BoltPacket p = PacketFactory.createPacket(p1_data);
        NegativeAcknowledgement p2 = (NegativeAcknowledgement) p;
        assertEquals(p1, p2);


    }

    @Test
    public void testShutdown() throws IOException {
        Shutdown p1 = new Shutdown();
        p1.setDestinationID(3);

        byte[] p1_data = p1.getEncoded();

        BoltPacket p = PacketFactory.createPacket(p1_data);
        Shutdown p2 = (Shutdown) p;
        assertEquals(p1, p2);
    }

    @Test
    public void testPacketUtil() throws Exception {
        InetAddress i = InetAddress.getLocalHost();
        byte[] enc = PacketUtil.encode(i);
        print(enc);
        InetAddress i2 = PacketUtil.decodeInetAddress(enc, 0, false);
        System.out.println(i2);
        assertEquals(i, i2);
    }


}
