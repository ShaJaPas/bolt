package bolt.xcoder;

import bolt.packets.DataPacket;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by keen on 27/02/16.
 */
@SuppressWarnings("unchecked")
public class XCoderRepository {

    private final Map<Class<?>, XCoderChain<?>> classXCoders = new ConcurrentHashMap<>();

    private final Map<Integer, Class<?>> idsToClass = new ConcurrentHashMap<>();

    private final AtomicInteger idSeq = new AtomicInteger(0);

    private final MessageAssembleBuffer messageAssembleBuffer;

    private XCoderRepository(final MessageAssembleBuffer messageAssembleBuffer) {
        this.messageAssembleBuffer = messageAssembleBuffer;
    }

    public static XCoderRepository create(final MessageAssembleBuffer messageAssembleBuffer) {
        XCoderRepository x = new XCoderRepository(messageAssembleBuffer);
        x.register(byte[].class, XCoderChain.rawBytePackageChain());
        return x;
    }

    public <T> int register(final Class<T> clazz, final XCoderChain<T> xCoder) {
        final Integer classId = idSeq.getAndIncrement();
        classXCoders.put(clazz, xCoder);
        xCoder.setClassId(classId);
        idsToClass.put(classId, clazz);
        return classId;
    }

    public <T> T decode(final DataPacket data) throws NoSuchElementException {
        final Collection<DataPacket> readyForDecode = messageAssembleBuffer.addChunk(data);
        if (!readyForDecode.isEmpty()) {
            final int classId = data.getClassID();
            final XCoderChain<T> xCoder = getXCoder(classId);
            return xCoder.decode(readyForDecode);
        }
        return null;
    }

    public <T> Collection<DataPacket> encode(final T object) throws NoSuchElementException {
        final XCoderChain<T> xCoder = (XCoderChain<T>) getXCoder(object.getClass());
        final Collection<DataPacket> encoded = xCoder.encode(object);

        boolean isMessage = encoded.stream().anyMatch(DataPacket::isMessage);
        if (isMessage) {
            final int messageId = messageAssembleBuffer.nextMessageId();
            encoded.forEach(p -> p.setMessageId(messageId));
        }

        return encoded;
    }

    private <T> XCoderChain<T> getXCoder(final Class<T> clazz) throws NoSuchElementException {
        final XCoderChain<T> xCoder = (XCoderChain<T>) classXCoders.get(clazz);
        if (xCoder == null) throw new NoSuchElementException("Class not found for class " + clazz);
        return xCoder;
    }

    private <T> XCoderChain<T> getXCoder(final int classId) throws NoSuchElementException {
        final Class<T> clazz = (Class<T>) idsToClass.get(classId);
        if (clazz == null) throw new NoSuchElementException("Class not found for id " + classId);
        return getXCoder(clazz);
    }

//    private int getClassId(final Class<?> clazz) throws NoSuchElementException {
//        final Integer classId = classToIds.get(clazz);
//        if (classId == null) throw new NoSuchElementException("No class id found for class " + clazz);
//        return classId;
//    }
//
//    private int getClassIdFromData(final byte[] data) {
//        return (data[0] & 0xFF) << 8 | (data[1] & 0xFF);
//    }
//
//    private byte[] getObjectData(final byte[] data) {
//        return Arrays.copyOfRange(data, CLASS_ID_BYTE_LENGTH, data.length);
//    }

}