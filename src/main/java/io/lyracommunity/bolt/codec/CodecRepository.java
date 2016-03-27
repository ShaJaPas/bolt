package io.lyracommunity.bolt.codec;

import io.lyracommunity.bolt.packet.DataPacket;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Created by keen on 27/02/16.
 */
@SuppressWarnings("unchecked")
public class CodecRepository {


    private final Map<Class<?>, CodecChain<?>> classXCoders = new ConcurrentHashMap<>();

    private final Map<Integer, Class<?>> idsToClass = new ConcurrentHashMap<>();

    private final AtomicInteger idSeq = new AtomicInteger(0);

    private final MessageAssembleBuffer messageAssembleBuffer;

    private CodecRepository(final MessageAssembleBuffer messageAssembleBuffer) {
        this.messageAssembleBuffer = messageAssembleBuffer;
    }

    public static CodecRepository create() {
        return new CodecRepository(new MessageAssembleBuffer());
    }

    public static CodecRepository basic(final MessageAssembleBuffer messageAssembleBuffer) {
        CodecRepository x = new CodecRepository(messageAssembleBuffer);
        x.register(byte[].class, CodecChain.rawBytePackageChain(messageAssembleBuffer));
        return x;
    }

    public <T> int register(final Class<T> clazz, final PacketCodec<T> xCoder) throws IllegalArgumentException {
        return register(clazz, CodecChain.of(messageAssembleBuffer, xCoder));
    }

    public <T> int register(final Class<T> clazz, final CodecChain<T> xCoder) throws IllegalArgumentException {
        if (classXCoders.containsKey(clazz)) throw new IllegalArgumentException("Class is already registered " + clazz);

        final Integer classId = idSeq.getAndIncrement();
        classXCoders.put(clazz, xCoder);
        xCoder.setClassId(classId);
        idsToClass.put(classId, clazz);
        return classId;
    }

    public <T> T decode(final DataPacket data) throws NoSuchElementException {
        final List<DataPacket> readyForDecode = messageAssembleBuffer.addChunk(data);
        if (!readyForDecode.isEmpty()) {
            final int classId = data.getClassID();
            final CodecChain<T> xCoder = getXCoder(classId);
            return xCoder.decode(readyForDecode);
        }
        return null;
    }

    public <T> Collection<DataPacket> encode(final T object) throws NoSuchElementException {
        final CodecChain<T> xCoder = (CodecChain<T>) getXCoder(object.getClass());
        return xCoder.encode(object);
    }

    private <T> CodecChain<T> getXCoder(final Class<T> clazz) throws NoSuchElementException {
        final CodecChain<T> xCoder = (CodecChain<T>) classXCoders.get(clazz);
        if (xCoder == null) throw new NoSuchElementException("Class not found for class " + clazz);
        return xCoder;
    }

    public <T> CodecChain<T> getXCoder(final int classId) throws NoSuchElementException {
        final Class<T> clazz = (Class<T>) idsToClass.get(classId);
        if (clazz == null) throw new NoSuchElementException("Class not found for id " + classId);
        return getXCoder(clazz);
    }

}
