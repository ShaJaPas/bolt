package bolt.xcoder;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Created by keen on 27/02/16.
 */
@SuppressWarnings("unchecked")
public class XCoderRepository {

    private static final int CLASS_ID_BYTE_LENGTH = 2;


    private final Map<Class<?>, XCoder<?>> classXCoders = new ConcurrentHashMap<>();

    private final Map<Class<?>, Integer> classToIds = new ConcurrentHashMap<>();

    private final Map<Integer, Class<?>> idsToClass = new ConcurrentHashMap<>();

    private final AtomicInteger idSeq = new AtomicInteger(0);

    public <T> int register(final Class<T> clazz, final XCoder<T> xCoder) {
        classXCoders.put(clazz, xCoder);
        Integer classId = idSeq.incrementAndGet();
        idsToClass.put(classId, clazz);
        classToIds.put(clazz, classId);
        return classId;
    }

    public <T> T decode(final byte[] data) throws NoSuchElementException {
        final int classId = getClassIdFromData(data);
        final byte[] objectData = getObjectData(data);
        final XCoder<T> xCoder = getXCoder(classId);
        return xCoder.decode(objectData);
    }

    public <T> Collection<byte[]> encode(final T object) throws NoSuchElementException {
        final XCoder<T> xCoder = (XCoder<T>) getXCoder(object.getClass());
        final int classId = getClassId(object.getClass());
        final Collection<byte[]> objectData = xCoder.encode(object);
        // TODO return an object with headers (reliable, isMessage, isFinalChunk, messageChunkNumber)
        return produceFinalData(classId, objectData);
    }

    private <T> XCoder<T> getXCoder(final Class<T> clazz) throws NoSuchElementException {
        final XCoder<T> xCoder = (XCoder<T>) classXCoders.get(clazz);
        if (xCoder == null) throw new NoSuchElementException("Class not found for class " + clazz);
        return xCoder;
    }

    private <T> XCoder<T> getXCoder(final int classId) throws NoSuchElementException {
        final Class<T> clazz = (Class<T>) idsToClass.get(classId);
        if (clazz == null) throw new NoSuchElementException("Class not found for id " + classId);
        return getXCoder(clazz);
    }

    private int getClassId(final Class<?> clazz) throws NoSuchElementException {
        final Integer classId = classToIds.get(clazz);
        if (classId == null) throw new NoSuchElementException("No class id found for class " + clazz);
        return classId;
    }

    private int getClassIdFromData(final byte[] data) {
        return (data[0] & 0xFF) << 8 | (data[1] & 0xFF);
    }

    private byte[] encodeClassId(final int classId) {
        return new byte[]{(byte) (classId >> 8), (byte) (classId)};
    }

    private Collection<byte[]> produceFinalData(final int classId, final Collection<byte[]> objectData) {
        return objectData.stream().map( d -> {
            byte[] data = new byte[d.length + 2];
            System.arraycopy(encodeClassId(classId), 0, data, 0, 2);
            System.arraycopy(d, 0, data, 2, d.length);
            return data;
        }).collect(Collectors.toList());
    }

    private byte[] getObjectData(final byte[] data) {
        return Arrays.copyOfRange(data, CLASS_ID_BYTE_LENGTH, data.length);
    }

}
