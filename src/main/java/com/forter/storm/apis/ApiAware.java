package com.forter.storm.apis;

import backtype.storm.topology.IRichBolt;

/**
 * Implemented by bolts who want to be aware of the API instrumentation command
 */
public interface ApiAware extends IRichBolt {}