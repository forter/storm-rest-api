package com.forter.storm.apis;

import com.forter.storm.apis.wrappers.ApisMorphedTuple;

import java.util.Set;

/**
 * Interface for letting join bolts be dynamically told what finish condition to set for current entry
 */
public interface ApiInstrumentedJoinBolt {
    public void executeExpected(ApisMorphedTuple morphedTuple, Set<String> fields);
}
