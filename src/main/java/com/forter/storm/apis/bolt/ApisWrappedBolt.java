package com.forter.storm.apis.bolt;

import com.google.common.base.Optional;

/**
 * Interface that supplies a way for bolt wrappers to be aware of the final bolt object they contain
 */
public interface ApisWrappedBolt {
    public <T> Optional<T> getWrappedInstance(Class<T> type);
}
