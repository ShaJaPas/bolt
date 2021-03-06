package io.lyracommunity.bolt.util;

import io.lyracommunity.bolt.packet.DataPacket;
import org.junit.Before;
import org.junit.Test;

import java.util.BitSet;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by keen on 26/03/16.
 */
public class DuplicateDetectorTest {

    private int size;
    private DuplicateDetector classUnderTest;


    @Before
    public void setUp() {
        setUp(10_000);
    }

    public void setUp(final int size) {
        final BitSet set = new BitSet(size);
        this.size = set.size();
        this.classUnderTest = DuplicateDetector.fromBitSet(set);
    }

    @Test
    public void checkDuplicatePacket_isDuplicate() throws Exception {
        assertFalse(classUnderTest.receivePacket(dataPacket(1)));
        assertTrue(classUnderTest.receivePacket(dataPacket(1)));
    }

    @Test
    public void checkDuplicatePacket_noDuplicates() throws Exception {
        assertFalse(classUnderTest.receivePacket(dataPacket(1)));
        assertFalse(classUnderTest.receivePacket(dataPacket(2)));
    }

    @Test
    public void checkDuplicatePacket_overflow() throws Exception {
        setUp(100);
        for (int i = 0; i < size * 3; i++) {
            assertFalse(classUnderTest.receivePacket(dataPacket(i)));
        }
        for (int i = 0; i < size; i++) {
            assertFalse(classUnderTest.receivePacket(dataPacket(i)));
        }
    }

    @Test
    public void checkDuplicatePacket_invalidateSegment() throws Exception {
        setUp(128);

        assertFalse(classUnderTest.receivePacket(firstPacketInSegment(0)));
        assertTrue(classUnderTest.receivePacket(firstPacketInSegment(0)));
        for (int i = 1; i < DuplicateDetector.DEFAULT_SEGMENT_COUNT; i++) {
            assertFalse(classUnderTest.receivePacket(firstPacketInSegment(i)));
        }
        assertFalse(classUnderTest.receivePacket(firstPacketInSegment(0)));
    }

    private DataPacket dataPacket(final int packetSeqNum) {
        final DataPacket dp = new DataPacket();
        dp.setPacketSeqNumber(packetSeqNum);
        return dp;
    }

    private DataPacket firstPacketInSegment(final int segmentNumber) {
        final DataPacket dp = new DataPacket();
        final int itemsPerSegment = size / DuplicateDetector.DEFAULT_SEGMENT_COUNT;
        dp.setPacketSeqNumber(segmentNumber * itemsPerSegment);
        return dp;
    }


}