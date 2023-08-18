package com.example.expiry;

import javax.cache.event.CacheEntryEvent;
import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListenerException;
import javax.cache.event.EventType;

/**
 *
 */
public class ExpiredFilter implements CacheEntryEventFilter<Object, Object> {
    /** {@inheritDoc} */
    @Override public boolean evaluate(CacheEntryEvent<?, ?> evt) throws CacheEntryListenerException {
        return evt.getEventType() == EventType.EXPIRED;
    }
}
