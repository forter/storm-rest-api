package com.forter.storm.apis;

import com.forter.storm.apis.errors.ApiTopologyErrorHandler;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
* Created by reem on 10/3/14.
*/
public class ApisTopologyConfigBuilder {
    protected final ApisTopologyConfig instance;

    public ApisTopologyConfigBuilder() {
        instance = new ApisTopologyConfig();
    }

    protected ApisTopologyConfigBuilder(ApisTopologyConfig instance) {
        this.instance = instance;
    }

    public ApisTopologyConfig build() {
        Preconditions.checkArgument(instance.apiSpout != null, "APIs stream must be given a default API spout");
        Preconditions.checkArgument(instance.defaultStreamSpouts != null, "APIs stream must be given default stream spouts");

        if (instance.apisStreamName == null) {
            instance.apisStreamName = "api-stream";
        }

        if (instance.apisCommandFieldName == null) {
            instance.apisCommandFieldName = "api-command";
        }

        if (instance.apisIdFieldName == null) {
            instance.apisIdFieldName = "request-id";
        }

        return instance;
    }

    public void setApiSpout(String apiSpout) {
        instance.apiSpout = apiSpout;
    }

    public void setDefaultStreamSpouts(List<String> defaultStreamSpouts) {
        instance.defaultStreamSpouts = defaultStreamSpouts;
    }

    public void setApisStreamName(String apisStreamName) {
        instance.apisStreamName = apisStreamName;
    }

    public void setErrorHandler(ApiTopologyErrorHandler errorHandler) {
        instance.errorHandler = errorHandler;
    }

    public void setApisCommandFieldName(String apisCommandFieldName) {
        instance.apisCommandFieldName = apisCommandFieldName;
    }

    public void setApisIdFieldName(String apisIdFieldName) {
        instance.apisIdFieldName = apisIdFieldName;
    }
}
