package com.forter.storm.apis.errors;

import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * An interface for letting topology decide what to do with API exceptions
 */
public interface ApiTopologyErrorHandler {
    ObjectNode getApiErrorMessage(String id, String message, Exception e);
}
