package bolt.xcoder;

/**
 * Created by omahoc9 on 3/1/16.
 */
public abstract class ObjectXCoder<T> implements XCoder<T, byte[]>
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
