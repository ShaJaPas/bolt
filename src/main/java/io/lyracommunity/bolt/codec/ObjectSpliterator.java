package io.lyracommunity.bolt.codec;

import java.util.Collection;

/**
 * Prepares objects to be sent as data packets by splitting them into smaller chunks.
 */
public interface ObjectSpliterator<T>
{

    /**
     * Splits the provided object into a collection of smaller transferable objects.
     *
     * @param object the object to split.
     * @return a list of result objects.
     */
    Collection<T> split(T object);

}
