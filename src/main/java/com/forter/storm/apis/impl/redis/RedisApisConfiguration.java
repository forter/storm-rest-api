package com.forter.storm.apis.impl.redis;

import com.forter.storm.apis.ApisTransportTopologyConfig;
import com.forter.storm.apis.errors.ApiTopologyErrorHandler;

/**
 * Created by reem on 10/7/14.
 */
public class RedisApisConfiguration implements ApisTransportTopologyConfig {
    String apisRedisResponseChannel;
    String apisRedisRequestQueue;
    String apisRedisHost;
    int apisRedisPort;
    private ApiTopologyErrorHandler errorHandler;

    public String getApisRedisResponseChannel() {
        return apisRedisResponseChannel;
    }

    public String getApisRedisRequestQueue() {
        return apisRedisRequestQueue;
    }

    public String getApisRedisHost() {
        return apisRedisHost;
    }

    public int getApisRedisPort() {
        return apisRedisPort;
    }

    public void setRedisResponseChannel(String redisResponseChannel) {
        this.apisRedisResponseChannel = redisResponseChannel;
    }

    public void setApisRedisRequestQueue(String apisRedisRequestQueue) {
        this.apisRedisRequestQueue = apisRedisRequestQueue;
    }

    public void setApisRedisHost(String host) {
        this.apisRedisHost = host;
    }

    public void setApisRedisPort(int port) {
        this.apisRedisPort = port;
    }

    @Override
    public ApiTopologyErrorHandler getErrorHandler() {
        return errorHandler;
    }

    public void setErrorHandler(ApiTopologyErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }
}
