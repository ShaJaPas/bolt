package io.lyracommunity.bolt.helper;

import io.lyracommunity.bolt.event.ReceiveObject;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by keen on 09/04/16.
 */
class PacketReceiver {

    private final Map<Class, AtomicInteger> totalReceived = new HashMap<>();

    void receive(final Object o) {
        final Class clazz = o.getClass();
        AtomicInteger maybeInt = totalReceived.get(clazz);
        if (maybeInt == null) totalReceived.putIfAbsent(clazz, new AtomicInteger(0));
        totalReceived.get(clazz).incrementAndGet();

        if (clazz.equals(ReceiveObject.class)) receive(((ReceiveObject) o).getPayload());
    }

    int getTotalReceived(final Class clazz) {
        final AtomicInteger r = totalReceived.get(clazz);
        return (r == null) ? 0 : r.get();
    }

}
