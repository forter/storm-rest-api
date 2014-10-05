package com.forter.storm.apis.errors;

import backtype.storm.tuple.Tuple;

/**
 * An interface for letting topology decide what to do with API exceptions
 */
public interface ApiTopologyErrorHandler {
    void reportApiError(String message, Exception e, Tuple input);
}
