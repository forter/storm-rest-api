package com.forter.storm.apis;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
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
    private final Map<String, Set<String>> joinBoltConditions = Maps.newLinkedHashMap();
    private final String id;
    private final long startTime;

    private List<Object> input;

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
                List<Object> tuple = Lists.newArrayList();

                ArrayNode tArray = (ArrayNode) boltOutputsJson.get(field);

                serializeTuple(tuple, tArray);

                this.boltOutputs.put(field, tuple);
            }
        }

        if (json.has("joinBoltConditions") && !json.get("joinBoltConditions").isNull()) {
            ObjectNode joinBoltConditions = (ObjectNode) json.get("joinBoltConditions");
            Iterator<String> fieldNamesIter = joinBoltConditions.fieldNames();
            while (fieldNamesIter.hasNext()) {
                String field = fieldNamesIter.next();
                Set<String> atts = Sets.newHashSet();

                ArrayNode arrayNode = (ArrayNode) joinBoltConditions.get(field);
                for (JsonNode node : arrayNode) {
                    atts.add(node.asText());
                }

                this.joinBoltConditions.put(field, atts);
            }
        }

        if (json.has("input") && !json.get("input").isNull()) {
            ArrayNode inputJson = (ArrayNode) json.get("input");
            ArrayList<Object> inputTuple = Lists.newArrayList();
            serializeTuple(inputTuple, inputJson);
            this.input = inputTuple;
        }
    }

    private void serializeTuple(List<Object> tuple, ArrayNode tArray) {
        for (JsonNode tNode : tArray) {
            if (tNode.isObject()) {
                tuple.add(tNode);
            } else {
                tuple.add(getObject(tNode));
            }
        }
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
     * When querying for single attributes, specify them here so that the join bolt would not time out
     * @param boltIdentification the ID for the join bolt in question (must implement ApiInstrumentedJoinBolt)
     * @param fields the fields that the join should wait for
     */
    public void addJoinWaitFor(String boltIdentification, Set<String> fields) {
        joinBoltConditions.put(boltIdentification, fields);
    }

    /**
     * Sets the default stream spout output. This should be a queue entry for the tx default stream.
     */
    public void setRawInput(ArrayList<Object> input) {
        this.input = input;
    }

    // Methods under this marker should only be used by the execution engine

    public boolean containsBolt(final String boltIdentification) {
        return boltPath.isEmpty() || boltPath.contains(boltIdentification);
    }

    public List<Object> getPredefinedBoltOutput(String sourceComponent) {
        return this.boltOutputs.get(sourceComponent);
    }

    public Set<String> getJoinWaitFor(String boltIdentification) {
        return joinBoltConditions.get(boltIdentification);
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
