package com.forter.storm.apis;

import com.forter.storm.apis.errors.ApiTopologyErrorHandler;

import java.io.Serializable;

/**
 * Created by reem on 4/13/15.
 */
public interface ApisTransportTopologyConfig extends Serializable {
    ApiTopologyErrorHandler getErrorHandler();
}
