package com.forter.storm.apis;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.javafx.tools.doclets.internal.toolkit.util.DocFinder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Spout for reading API requests from redis queue
 */
public abstract class ApiRequestSpout extends BaseRichSpout {
    private final static Logger logger = LoggerFactory.getLogger(ApiRequestSpout.class);

    private final ApisTopologyConfig config;

    private SpoutOutputCollector collector;
    private ObjectReader reader;

    public ApiRequestSpout(ApisTopologyConfig config) {
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
                    if (config.getErrorHandler() != null) {
                        reportError(id, config.getErrorHandler().getApiErrorMessage(id, message, e));
                    }
                }
            } catch (Exception e) {
                logger.warn("There was an error parsing API request.", e);
                logger.debug("Request source: {}", requestJson);
            }
        }
    }

    protected abstract void emitCommand(SpoutOutputCollector collector, ObjectNode request);

    /**
     * Method for adding extra values to emitted tuple, for overloading by implementations
     * @param values The initial values to be emitted, will be edited by overriding method
     */
    protected void appendExtraValues(Values values) { }

    /**
     * Method for adding extra fields to emitted tuple, for overloading by implementations
     * @param fields The initial fields to be emitted, will be edited by overriding method
     */
    protected void appendExtraFields(List<String> fields) { }

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
