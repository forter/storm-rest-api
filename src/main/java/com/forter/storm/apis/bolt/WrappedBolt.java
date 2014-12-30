package com.forter.storm.apis.bolt;

import backtype.storm.topology.IRichBolt;

/**
 * Created by reem on 12/30/14.
 */
public interface WrappedBolt {
    public IRichBolt getWrappedInstance();
}
