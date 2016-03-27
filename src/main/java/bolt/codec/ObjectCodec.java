package bolt.codec;

/**
 * Created by omahoc9 on 3/1/16.
 */
public abstract class ObjectCodec<T> implements Codec<T, byte[]>
{

    private int classId;

    public int getClassId()
    {
        return classId;
    }

    public void setClassId(int classId)
    {
        this.classId = classId;
    }

}
