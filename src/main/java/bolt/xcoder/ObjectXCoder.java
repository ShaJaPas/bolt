package bolt.xcoder;

/**
 * Created by omahoc9 on 3/1/16.
 */
public abstract class ObjectXCoder<T> implements XCoder<T, byte[]>
{

    //TODO consider moving this out of class.
    private final boolean reliable;

    private int classId;

    public ObjectXCoder(boolean reliable)
    {
        this.reliable = reliable;
    }

    public boolean isReliable()
    {
        return reliable;
    }

    public int getClassId()
    {
        return classId;
    }

    public void setClassId(int classId)
    {
        this.classId = classId;
    }

//    @Override
//    public final byte[] encode(T object)
//    {
//        final byte[] objectData = encodeObjectData(object);
//        byte[] data = new byte[objectData.length + 2];
//        System.arraycopy(encodeClassId(getClassId()), 0, data, 0, 2);
//        System.arraycopy(objectData, 0, data, 2, objectData.length);
//        return data;
//    }
//
//    protected abstract byte[] encodeObjectData(final T object);
//
//    protected byte[] encodeClassId(final int classId) {
//        return new byte[]{(byte) (classId >> 8), (byte) (classId)};
//    }
//



}
