package com.forter.storm.apis;

import com.google.common.base.Preconditions;

import java.util.List;

/**
* Created by reem on 10/3/14.
*/
public class ApisRemoteCommandTopologyConfigBuilder {
    protected final ApisRemoteCommandTopologyConfig instance;

    public ApisRemoteCommandTopologyConfigBuilder() {
        instance = new ApisRemoteCommandTopologyConfig();
    }

    protected ApisRemoteCommandTopologyConfigBuilder(ApisRemoteCommandTopologyConfig instance) {
        this.instance = instance;
    }

    public ApisTopologyConfig build() {
        Preconditions.checkArgument(instance.apiSpout != null, "APIs stream must be given a default API spout");
        Preconditions.checkArgument(instance.defaultStreamSpouts != null, "APIs stream must be given default stream spouts");
        Preconditions.checkArgument(instance.trasport != null, "Some API transport must exist");

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

    public void setApisCommandFieldName(String apisCommandFieldName) {
        instance.apisCommandFieldName = apisCommandFieldName;
    }

    public void setApisIdFieldName(String apisIdFieldName) {
        instance.apisIdFieldName = apisIdFieldName;
    }

    public void setTrasport(ApisTransportTopologyConfig trasport) {
        instance.trasport = trasport;
    }
}
