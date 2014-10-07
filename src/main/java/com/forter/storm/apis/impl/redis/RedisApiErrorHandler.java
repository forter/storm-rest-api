package com.forter.storm.apis.impl.redis;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.storm.apis.ObjectMapperHolder;
import com.forter.storm.apis.errors.ApiTopologyErrorHandler;

import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;

/**
 * Error handler for topology errors in API stream
 */
public class RedisApiErrorHandler implements ApiTopologyErrorHandler, Serializable {
    @Override
    public ObjectNode getApiErrorMessage(String id, String message, Exception e) {
        final ObjectNode errorJson = (ObjectNode) ObjectMapperHolder.getReader().createObjectNode();

        errorJson.put("id", id);
        errorJson.put("error", true);
        errorJson.put("errorMessage", message);

        if (e != null) {
            ObjectNode detailedError = errorJson.putObject("detailedError");
            detailedError.put("exception", e.getClass().getCanonicalName());
            detailedError.put("message", e.getMessage());

            StringWriter stackWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stackWriter));
            detailedError.put("stackTrace", stackWriter.toString());
        }

        return errorJson;
    }
}
