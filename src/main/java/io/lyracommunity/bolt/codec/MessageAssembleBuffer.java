package io.lyracommunity.bolt.codec;

import io.lyracommunity.bolt.packet.DataPacket;
import io.lyracommunity.bolt.packet.PacketUtil;
import io.lyracommunity.bolt.util.SeqNum;

import java.util.*;

/**
 * Created by keen on 29/02/16.
 */
public class MessageAssembleBuffer
{

    private volatile int messageId = 0;

    private Map<Integer, MessageChunks> messageMap = new HashMap<>();

    public List<DataPacket> addChunk(final DataPacket dataPacket) {
        if (!dataPacket.isMessage()) {
            return Collections.singletonList(dataPacket);
        }
        else {
            List<DataPacket> result = getOrCreate(dataPacket.getMessageId()).addChunk(dataPacket);
            if (!result.isEmpty()) messageMap.remove(dataPacket.getMessageId());  // remove if complete.
            return result;
        }
    }

    private MessageChunks getOrCreate(final int messageId) {
        messageMap.putIfAbsent(messageId, new MessageChunks());
        return messageMap.get(messageId);
    }

    int nextMessageId() {
        return messageId = SeqNum.increment(messageId, PacketUtil.MAX_MESSAGE_ID);
    }

    public void clear() {
        messageMap.clear();
    }

    private static class MessageChunks {
        private final Map<Integer, DataPacket> received = new HashMap<>();
        private int totalChunks;

        private List<DataPacket> addChunk(final DataPacket packet) {
            received.put(packet.getMessageChunkNumber(), packet);
            if (packet.isFinalMessageChunk()) {
                totalChunks = packet.getMessageChunkNumber() + 1;
            }
            return totalChunks == received.size()
                    ? new ArrayList<>(received.values())
                    : Collections.emptyList();
        }
    }

}
