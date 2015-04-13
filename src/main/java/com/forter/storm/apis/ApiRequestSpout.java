package com.forter.storm.apis;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.base.BaseRichSpout;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.storm.apis.errors.ApiTopologyErrorHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Spout for reading API requests from redis queue
 */
public abstract class ApiRequestSpout<T extends ApisTopologyConfig> extends BaseRichSpout {
    private final static Logger logger = LoggerFactory.getLogger(ApiRequestSpout.class);

    private final T config;

    private SpoutOutputCollector collector;
    private ObjectReader reader;

    public ApiRequestSpout(T config) {
        this.config = config;
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        this.reader = ObjectMapperHolder.getReader();
    }

    @Override
    public void nextTuple() {
        String requestJson = getApiCommandJson();
        if (requestJson != null) {
            try {
                JsonNode request = this.reader.readTree(requestJson);
                String id = request.get("id").asText();
                try {
                    emitCommand(collector, (ObjectNode) request);
                } catch (Exception e) {
                    String message = "An error has ocurred while executin API call";
                    ApiTopologyErrorHandler errorHandler = config.getTransport().getErrorHandler();
                    if (errorHandler != null) {
                        reportError(id, errorHandler.getApiErrorMessage(id, message, e));
                    }
                }
            } catch (Exception e) {
                logger.warn("There was an error parsing API request.", e);
                logger.debug("Request source: {}", requestJson);
            }
        }
    }

    protected abstract void emitCommand(SpoutOutputCollector collector, ObjectNode request);

    protected abstract void reportError(String id, ObjectNode error);

    protected abstract String getApiCommandJson();

    @Override
    public void ack(Object msgId) {
        super.ack(msgId);
    }

    @Override
    public void fail(Object msgId) {
        super.fail(msgId);
    }

}
