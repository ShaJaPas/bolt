package bolt.xcoder;

import bolt.packets.DataPacket;
import bolt.util.SequenceNumber;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by keen on 29/02/16.
 */
public class MessageAssembleBuffer
{

    private static final int MAX_MESSAGE_ID = (int) Math.pow(2, 16) - 1;

    private volatile int messageId = 0;

    private Map<Integer, Message> messageMap = new HashMap<>();

    public Collection<DataPacket> addChunk(final DataPacket dataPacket) {
        if (!dataPacket.isMessage()) {
            return Collections.singletonList(dataPacket);
        }
        else {
            messageMap.putIfAbsent(dataPacket.getMessageId(), new Message());
            return messageMap.get(dataPacket.getMessageId()).addChunk(dataPacket);
        }
    }

    public int nextMessageId() {
        return SequenceNumber.increment(messageId, MAX_MESSAGE_ID);
    }


    private static class Message {
        private final Map<Integer, DataPacket> received = new HashMap<>();
        private int totalChunks;

        private Collection<DataPacket> addChunk(final DataPacket packet) {
            received.put(packet.getMessageChunkNumber(), packet);
            if (packet.isFinalMessageChunk()) {
                totalChunks = packet.getMessageChunkNumber();
            }
            return totalChunks == received.size()
                    ? new ArrayList<>(received.values())
                    : Collections.emptyList();
        }

    }

}
