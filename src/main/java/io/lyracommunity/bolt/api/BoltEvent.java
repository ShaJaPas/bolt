package io.lyracommunity.bolt.api;

import io.lyracommunity.bolt.api.event.BoltEventType;

/**
 * Marker interface for a Bolt event.
 *
 * @author Cian.
 */
public interface BoltEvent {

    /**
     * @return the type of event this is.
     */
    BoltEventType getEventType();

}
