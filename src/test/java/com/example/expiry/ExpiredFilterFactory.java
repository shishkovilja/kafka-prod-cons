package com.example.expiry;

import javax.cache.configuration.Factory;

/**
 *
 */
public class ExpiredFilterFactory implements Factory<ExpiredFilter> {
    /** {@inheritDoc} */
    @Override public ExpiredFilter create() {
        return new ExpiredFilter();
    }
}
