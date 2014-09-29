package com.forter.storm.apis;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;

import java.util.*;

/**
 * Class that encapsulates an API command that will go through the topology
 */
public class ApisTopologyCommand {
    private final List<String> boltPath;
    private final Map<String, List<Object>> boltOutputs = Maps.newLinkedHashMap();
    private final Map<String, Set<String>> joinBoltConditions = Maps.newLinkedHashMap();
    private List<Object> input;

    /**
     * runs entire topology
     */
    public ApisTopologyCommand() {
        this(null);
    }

    /**
     * create a command specifying a path of bolts to traverse
     * @param boltPath this list will contain all bolts this command will invoke. Specifying wrong values will lead to
     *                 tuples timing out in-flight by the topology
     */
    public ApisTopologyCommand(List<String> boltPath) {
        if (boltPath != null) {
            this.boltPath = boltPath;
        } else {
            this.boltPath = null;
        }
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
        return boltPath == null || Iterables.any(boltPath, new Predicate<String>() {
            @Override
            public boolean apply(String input) {
                return input.equals(boltIdentification);
            }
        });
    }

    public List<Object> getPredefinedBoltOutput(String sourceComponent) {
        return this.boltOutputs.get(sourceComponent);
    }

    public Set<String> getJoinWaitFor(String boltIdentification) {
        return joinBoltConditions.get(boltIdentification);
    }

    public List<Object> getRawInput() {
        return input;
    }
}
