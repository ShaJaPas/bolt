package bolt.xcoder;

import bolt.packets.DataPacket;
import bolt.util.MessageAssembleBuffer;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by omahoc9 on 3/1/16.
 */
public class PackageChain<T>
{

    private final ObjectSpliterator<T>  spliterator;
    private final PacketXCoder<T>       packetXCoder;
    private final MessageAssembleBuffer messageAssembleBuffer;

    private PackageChain(final ObjectSpliterator<T> spliterator, final PacketXCoder<T> packetXCoder)
    {
        this.spliterator = spliterator;
        this.packetXCoder = packetXCoder;
        this.messageAssembleBuffer = new MessageAssembleBuffer();
    }

    public T decode(final DataPacket data)
    {
        final Collection<DataPacket> readyForDecode = messageAssembleBuffer.addChunk(data);
        if (!readyForDecode.isEmpty()) {
            return packetXCoder.decode(readyForDecode);
        }
        return null;
    }

    public Collection<DataPacket> encode(T object)
    {
        final Collection<T> split = (spliterator != null) ? spliterator.split(object) : Collections.singletonList(object);

        final List<DataPacket> packets = split.stream()
                .flatMap(t -> packetXCoder.encode(t).stream())
                .collect(Collectors.toList());

        boolean isMessage = packets.stream().anyMatch(DataPacket::isMessage);
        if (isMessage) {
            final int messageId = messageAssembleBuffer.nextMessageId();
            packets.forEach(p -> p.setMessageId(messageId));
        }
        return packets;
    }

    public void setClassId(final int classId) {
        packetXCoder.setClassId(classId);
    }

}
