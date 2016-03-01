package bolt.xcoder;

import java.util.Collection;

/**
 * Created by keen on 27/02/16.
 */
public interface XCoder<T> {

    /**
     * Decodes an arbitrary-length array of bytes into its original
     * object.
     *
     * @param data array of bytes to decode.
     * @return the original, decoded object.
     */
    T decode(byte[] data);

    /**
     * Encodes an object into a collection of byte chunks.
     * <p>
     * These chunks can subsequently be re-assembled and decoded
     * in order to get the original object.
     *
     * @param object the object to encode.
     * @return a list of byte chunks.
     */
    Collection<byte[]> encode(T object);

}
