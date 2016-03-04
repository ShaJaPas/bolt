package bolt.util;

import java.util.List;

/**
 * Circular array: the most recent value overwrites the oldest one if there is no more free
 * space in the array.
 */
public class CircularArray<T> {

    protected final int max;
    private final Object[] circularArray;
    protected int position = 0;
    protected boolean haveOverflow = false;

    /**
     * Create a new circularArray of the given size.
     *
     * @param size maximum size of array.
     */
    public CircularArray(final int size) {
        max = size;
        circularArray = new Object[size];
    }

    /**
     * Add an entry.
     */
    public void add(T entry) {
        if (position >= max) {
            position = 0;
            haveOverflow = true;
        }
        circularArray[position] = entry;
        position++;
    }

    /**
     * Returns the number of elements in this list.
     */
    public int size() {
        return haveOverflow ? max : Math.min(position, max);
    }

    public String toString() {
        return circularArray.toString();
    }

    @SuppressWarnings("unchecked")
    protected T[] getArray() {
        return (T[]) circularArray;
    }

    @SuppressWarnings("unchecked")
    protected T getEntry(final int index) {
        return (T) circularArray[index];
    }

}
