package bolt.xcoder;

import bolt.packets.DataPacket;

import java.util.Arrays;
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

//    private static final int CLASS_ID_BYTE_LENGTH = 2;


    private final Map<Class<?>, PackageChain<?>> classXCoders = new ConcurrentHashMap<>();

    private final Map<Class<?>, Integer> classToIds = new ConcurrentHashMap<>();

    private final Map<Integer, Class<?>> idsToClass = new ConcurrentHashMap<>();

    private final AtomicInteger idSeq = new AtomicInteger(0);

    public <T> int register(final Class<T> clazz, final PackageChain<T> xCoder) {
        final Integer classId = idSeq.incrementAndGet();
        classXCoders.put(clazz, xCoder);
        xCoder.setClassId(classId);
        idsToClass.put(classId, clazz);
        classToIds.put(clazz, classId);
        return classId;
    }

    public <T> T decode(final DataPacket data) throws NoSuchElementException {
        final int classId = data.getClassID();
        final PackageChain<T> xCoder = getXCoder(classId);
        return xCoder.decode(data);
    }

    public <T> Collection<DataPacket> encode(final T object) throws NoSuchElementException {
        final PackageChain<T> xCoder = (PackageChain<T>) getXCoder(object.getClass());
        return xCoder.encode(object);
    }

    private <T> PackageChain<T> getXCoder(final Class<T> clazz) throws NoSuchElementException {
        final PackageChain<T> xCoder = (PackageChain<T>) classXCoders.get(clazz);
        if (xCoder == null) throw new NoSuchElementException("Class not found for class " + clazz);
        return xCoder;
    }

    private <T> PackageChain<T> getXCoder(final int classId) throws NoSuchElementException {
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
