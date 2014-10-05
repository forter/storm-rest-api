package com.forter.storm.apis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.*;

/**
 * Class that encapsulates an API command that will go through the topology
 */
public class ApisTopologyCommand {
    private final Set<String> boltPath = Sets.newHashSet();
    private final Map<String, List<Object>> boltOutputs = Maps.newLinkedHashMap();
    private String id;
    private long startTime;

    private List<Object> input;

    public ApisTopologyCommand() {}

    public ApisTopologyCommand(ObjectNode json) {
        this(json.get("id").asText(), null);

        if (json.has("boltPath") && !json.get("boltPath").isNull()) {
            ArrayNode boltPathJson = (ArrayNode) json.get("boltPath");
            for (JsonNode node : boltPathJson) {
                this.boltPath.add(node.asText());
            }
        }

        if (json.has("boltOutputs") && !json.get("boltOutputs").isNull()) {
            ObjectNode boltOutputsJson = (ObjectNode) json.get("boltOutputs");

            Iterator<String> fieldNamesIter = boltOutputsJson.fieldNames();
            while (fieldNamesIter.hasNext()) {
                String field = fieldNamesIter.next();
                ArrayNode tupleArray = (ArrayNode) boltOutputsJson.get(field);
                this.boltOutputs.put(field, deserializeTuple(tupleArray));
            }
        }

        if (json.has("input") && !json.get("input").isNull()) {
            ArrayNode tupleArray = (ArrayNode) json.get("input");
            this.input = deserializeTuple(tupleArray);
        }
    }

    private List<Object> deserializeTuple(ArrayNode tArray) {
        ArrayList<Object> tuple = Lists.newArrayList();
        for (JsonNode tNode : tArray) {
            if (tNode.isObject()) {
                tuple.add(tNode);
            } else {
                tuple.add(getObject(tNode));
            }
        }
        return tuple;
    }

    public static Object getObject(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isDouble()) {
            return node.asDouble();
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isInt()) {
            return node.asInt();
        }
        if (node.isLong()) {
            return node.asLong();
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        throw new UnsupportedOperationException(
                String.format("The json node did not contain a familiar data type: %s", node.getClass()));
    }

    /**
     * runs entire topology
     */
    public ApisTopologyCommand(String id) {
        this(id, null);
    }

    /**
     * create a command specifying a path of bolts to traverse
     * @param boltPath this list will contain all bolts this command will invoke. Specifying wrong values will lead to
     *                 tuples timing out in-flight by the topology
     */
    public ApisTopologyCommand(String id, List<String> boltPath) {
        this.id = id;
        if (boltPath != null) {
            this.boltPath.addAll(boltPath);
        }
        this.startTime = System.currentTimeMillis();
    }

    /**
     * used to pre-define a bolt's output for situations where the entry point is in the middle of the topology
     * @param boltIdentification the bolt's name
     * @param tuple the tuple that will be defined as the bolt's output
     */
    public void setPredefinedBoltOutput(String boltIdentification, List<Object> tuple) {
        this.boltOutputs.put(boltIdentification, tuple);
    }

    /**
     * Sets the default stream spout output. This should be a queue entry for the tx default stream.
     */
    public void setInput(ArrayList<Object> input) {
        this.input = input;
    }

    // Methods under this marker should only be used by the execution engine

    public boolean containsBolt(final String boltIdentification) {
        return boltPath.isEmpty() || boltPath.contains(boltIdentification);
    }

    public List<Object> getPredefinedBoltOutput(String sourceComponent) {
        return this.boltOutputs.get(sourceComponent);
    }

    public List<Object> getInput() {
        return input;
    }

    public String getId() {
        return id;
    }

    public long getStartTime() {
        return startTime;
    }
}
