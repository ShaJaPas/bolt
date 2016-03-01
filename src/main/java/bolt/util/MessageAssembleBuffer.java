package bolt.util;

import bolt.packets.DataPacket;

import java.util.Collection;
import java.util.Collections;

/**
 * Created by keen on 29/02/16.
 */
public class MessageAssembleBuffer
{

    private static final int MAX_MESSAGE_ID = (int) Math.pow(2, 16) - 1;

    private volatile int messageId = 0;

    public Collection<DataPacket> addChunk(final DataPacket dataPacket) {
        if (!dataPacket.isMessage()) {
            return Collections.singletonList(dataPacket);
        }
        else {
            //TODO implement
            // Add chunk

            // If all chunks exist, return them as collection

            // Else return empty list
        }
    }

    public int nextMessageId() {
        return SequenceNumber.increment(messageId, MAX_MESSAGE_ID);
    }

}
