package com.forter.storm.apis.impl.redis;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.storm.apis.ApiRequestSpout;
import com.forter.storm.apis.ApisTopologyCommand;
import com.forter.storm.apis.ApisTopologyConfig;
import com.forter.storm.apis.ObjectMapperHolder;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Spout for running the inference model on a given input.
 */
public class RedisModelApiSpout extends RedisApiSpout {
    private final static Logger logger = LoggerFactory.getLogger(ApiRequestSpout.class);
    private static String MODEL_STREAM_NAME = "api-stream-model";
    private SpoutOutputCollector collector;
    private ObjectReader reader;

    public RedisModelApiSpout(ApisTopologyConfig config) {
        super(config);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        List<String> declaredFields = Lists.newArrayList(config.getApisIdFieldName(), config.getApisCommandFieldName());

        appendExtraFields(declaredFields);

        declarer.declareStream(MODEL_STREAM_NAME, new Fields(declaredFields));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
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
                    Map<String, Object> attributes = createAttributes(request);
                    String uuid = UUID.randomUUID().toString();

                    Values values = new Values(id, attributes);
                    appendExtraValues(values);

                    this.collector.emit(MODEL_STREAM_NAME, values, uuid);
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


    @Override
    protected ApisTopologyCommand createCommand(ObjectNode request) {
        return null;
    }

    private Map<String, Object> createAttributes(JsonNode request) {
        Map<String, Object> attributes = new HashMap<>();
        ObjectNode json = (ObjectNode) request;
        ArrayNode tupleArray = (ArrayNode) json.get("input");
        String modelName = tupleArray.get(0).asText();
        Map<String, String> modelAttributes = modelAttributesJsonToMap(tupleArray.get(1));
        attributes.put("modelName", modelName);
        attributes.put("decisionModel", modelAttributes);
        return attributes;
    }

    private Map<String, String> modelAttributesJsonToMap(JsonNode modelAttributes) {
        Map<String, String> modelAttributesMap = new HashMap();
        Iterator<Map.Entry<String, JsonNode>> iterator = modelAttributes.fields();

        while (iterator.hasNext()) {
            Map.Entry<String, JsonNode> attr = iterator.next();
            modelAttributesMap.put(attr.getKey(), attr.getValue().asText());
        }


        return modelAttributesMap;
    }


}
