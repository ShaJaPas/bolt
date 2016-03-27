package bolt.codec;

/**
 * Created by keen on 27/02/16.
 */
public interface Codec<D, E> {

    /**
     * Decodes an arbitrary-length array of bytes into its original
     * object.
     *
     * @param data array of bytes to decode.
     * @return the original, decoded object.
     */
    D decode(E data);

    /**
     * Encodes an object into a collection of byte chunks.
     * <p>
     * These chunks can subsequently be re-assembled and decoded
     * in order to get the original object.
     *
     * @param object the object to encode.
     * @return a list of byte chunks.
     */
    E encode(D object);

}
