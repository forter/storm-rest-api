package com.forter.storm.apis.impl.redis;

import com.forter.storm.apis.ApisTopologyConfig;

/**
 * Created by reem on 10/7/14.
 */
public class RedisApisConfiguration extends ApisTopologyConfig {
    String apisRedisResponseChannel;
    String apisRedisRequestQueue;
    String apisRedisHost;
    int apisRedisPort;

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
}
