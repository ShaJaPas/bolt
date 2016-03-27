package io.lyracommunity.bolt.packet;

import java.util.EnumSet;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;

/**
 * Created by omahoc9 on 3/18/16.
 */
public enum DeliveryType
{

    /**
     * Type 0: Fastest, unreliable, out-of-order, single-packet delivery.
     */
    UNRELIABLE_UNORDERED((byte) 0, false, false, false),

    /**
     * Type 1: Fast, reliable, out-of-order, single-packet delivery.
     */
    RELIABLE_UNORDERED((byte) 1, true, false, false),

    /**
     * Type 2: Fast reliable, out-of-order delivery with message (dis)assembling.
     */
    RELIABLE_UNORDERED_MESSAGE((byte) 2, true, false, true),

    /**
     * Type 3: Reliable, ordered, single-packet delivery.
     */
    RELIABLE_ORDERED((byte) 3, true, true, false),

    /**
     * Type 4: Reliable, ordered delivery with message (dis)assembling.
     */
    RELIABLE_ORDERED_MESSAGE((byte) 4, true, true, true);

    private final byte id;
    private final boolean reliable;
    private final boolean ordered;
    private final boolean message;


    DeliveryType(final byte id, final boolean reliable, final boolean ordered, final boolean message)
    {
        this.id = id;
        this.reliable = reliable;
        this.ordered = ordered;
        this.message = message;
    }

    public static DeliveryType fromId(final byte id) {
        return BY_ID.get((int) id);
    }

    public byte getId()
    {
        return id;
    }

    public boolean isReliable()
    {
        return reliable;
    }

    public boolean isOrdered()
    {
        return ordered;
    }

    public boolean isMessage()
    {
        return message;
    }

    public DeliveryType toNonMessage() {
        if (this == RELIABLE_ORDERED_MESSAGE) return RELIABLE_ORDERED;
        if (this == RELIABLE_UNORDERED_MESSAGE) return RELIABLE_UNORDERED;
        return this;
    }

    @Override
    public String toString()
    {
        final StringBuilder b = new StringBuilder("DeliveryType{");
        if (reliable) b.append("reliable");
        if (ordered) b.append("|ordered");
        if (message) b.append("|message");
        return b.append("}").toString();
    }

    // -------- STATIC HELPERS ------- //

    private static Map<Integer, DeliveryType> BY_ID = buildById();

    private static Map<Integer, DeliveryType> buildById()
    {
        return EnumSet.allOf(DeliveryType.class).stream().collect(Collectors.toMap(d -> (int) d.getId(), identity()));
    }
}
