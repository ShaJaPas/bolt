package io.lyracommunity.bolt.helper;

import io.lyracommunity.bolt.api.event.Message;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Receive counter by class.
 *
 * @author Cian.
 */
class PacketReceiver {

    private final Map<Class, AtomicInteger> totalReceived = new ConcurrentHashMap<>();

    void receive(final Object o) {
        final Class clazz = o.getClass();
        final AtomicInteger maybeInt = totalReceived.get(clazz);
        if (maybeInt == null) totalReceived.putIfAbsent(clazz, new AtomicInteger(0));
        totalReceived.get(clazz).incrementAndGet();

        if (clazz.equals(Message.class)) receive(((Message) o).getPayload());
    }

    int getTotalReceived(final Class clazz) {
        final AtomicInteger r = totalReceived.get(clazz);
        return (r == null) ? 0 : r.get();
    }

}
