package bolt.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Circular array: the most recent value overwrites the oldest one if there is no more free
 * space in the array.
 */
public class CircularArray<T> {

    protected final int max;
    protected final List<T> circularArray;
    protected int position = 0;
    protected boolean haveOverflow = false;

    /**
     * Create a new circularArray of the given size
     *
     * @param size
     */
    public CircularArray(int size) {
        max = size;
        circularArray = new ArrayList<>(size);
    }

    /**
     * add an entry
     */
    public void add(T entry) {
        if (position >= max) {
            position = 0;
            haveOverflow = true;
        }
        if (circularArray.size() > position) {
            circularArray.remove(position);
        }
        circularArray.add(position, entry);
        position++;
    }

    /**
     * Returns the number of elements in this list
     */
    public int size() {
        return circularArray.size();
    }

    public String toString() {
        return circularArray.toString();
    }

}
