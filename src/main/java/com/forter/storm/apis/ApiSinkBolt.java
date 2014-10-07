package com.forter.storm.apis;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.storm.apis.bolt.ApiAware;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Bolt for receiving end results for API stream operations and publishing them on redis for consumers
 */
public abstract class ApiSinkBolt implements IRichBolt, ApiAware {
    private final static Logger logger = LoggerFactory.getLogger(ApiSinkBolt.class);

    protected ApisTopologyConfig apisConfiguration;

    private OutputCollector collector;
    private ObjectReader reader;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.reader = ObjectMapperHolder.getReader();
    }

    @Override
    public void execute(Tuple input, ApisTopologyCommand command) {
        if (command == null) {
            this.collector.ack(input);
            return;
        }

        final String id = command.getId();
        try {
            final ObjectNode response = createResponse(input, command);
            registerApiResult(id, response);
            this.collector.ack(input);
        } catch (Exception e) {
            logger.warn("Error writing API results to redis, writing error", e);
            try {
                if (apisConfiguration.getErrorHandler() != null) {
                    apisConfiguration.getErrorHandler()
                            .reportApiError(id, "Error writing API results to redis, writing error", e, input);
                }
            } catch (Exception e1) {
                logger.warn("Error reporting error to redis, API will not get a response", e1);
            }
            this.collector.fail(input);
        }
    }

    protected abstract void registerApiResult(String id, ObjectNode response);

    @Override
    public void setApiConfiguration(ApisTopologyConfig apisConfiguration) {
        this.apisConfiguration = apisConfiguration;
    }

    @Override
    public void execute(Tuple input) {
        this.execute(input, null);
    }

    private ObjectNode createResponse(Tuple input, ApisTopologyCommand command) {
        ObjectNode response = (ObjectNode) reader.createObjectNode();
        response.put("error", false);
        response.put("id", command.getId());
        response.put("took", System.currentTimeMillis() - command.getStartTime());
        ObjectNode tuple = response.putObject("tuple");

        for (String field : input.getFields()) {
            if (apisConfiguration.getApisCommandFieldName().equals(field) ||
                    apisConfiguration.getApisIdFieldName().equals(field)) {
                continue;
            }
            Object value = input.getValueByField(field);
            putObject(tuple, field, value);
        }
        return response;
    }

    public static void putObject(ObjectNode json, String field, Object valueObject) {
        // TODO: find a better way of doing this...
        if (valueObject == null) {
            json.putNull(field);
        } else if (valueObject instanceof String) {
            json.put(field, (String) valueObject);
        } else if (valueObject instanceof Double) {
            json.put(field, (Double) valueObject);
        } else if (valueObject instanceof Integer) {
            json.put(field, (Integer) valueObject);
        } else if (valueObject instanceof Boolean) {
            json.put(field, (Boolean) valueObject);
        } else if (valueObject instanceof Long) {
            json.put(field, (Long) valueObject);
        } else if (valueObject instanceof JsonNode) {
            json.set(field, ((JsonNode) valueObject));
        } else {
            throw new IllegalArgumentException(String.format("Type of tuple property %s (%s) cannot be inserted into JSON",
                    field, valueObject.getClass().getName()));
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    @Override
    public Map<String, Object> getComponentConfiguration() { return null; }
}
