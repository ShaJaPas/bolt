package bolt.receiver;

/**
 * Created by keen on 06/03/16.
 */
public class RoutedData {

    private final long sourceId;

    private final Object payload;

    public RoutedData(final long sourceId, final Object payload) {
        this.sourceId = sourceId;
        this.payload = payload;
    }

    public long getSourceId() {
        return sourceId;
    }

    public Object getPayload() {
        return payload;
    }

    public boolean isOfType(final Class<?> expected) {
        return payload != null && expected.equals(payload.getClass());
    }

    public boolean isOfSubType(final Class<?> expected) {
        return payload != null && expected.isAssignableFrom(payload.getClass());
    }

}
