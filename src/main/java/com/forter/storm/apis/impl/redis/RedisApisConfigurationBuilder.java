package com.forter.storm.apis.impl.redis;

import com.forter.storm.apis.ApisTopologyConfig;
import com.forter.storm.apis.ApisTopologyConfigBuilder;

/**
* Created by reem on 10/3/14.
*/
public class RedisApisConfigurationBuilder extends ApisTopologyConfigBuilder {
    RedisApisConfiguration instance;

    public RedisApisConfigurationBuilder() {
        this(new RedisApisConfiguration());
    }

    public RedisApisConfigurationBuilder(RedisApisConfiguration instance) {
        super(instance);
        this.instance = (RedisApisConfiguration) super.instance;
    }

    public void setRedisResponseChannel(String redisResponseChannel) {
        this.instance.apisRedisResponseChannel = redisResponseChannel;
    }

    public void setApisRedisRequestQueue(String apisRedisRequestQueue) {
        this.instance.apisRedisRequestQueue = apisRedisRequestQueue;
    }

    public void setApisRedisHost(String host) {
        this.instance.apisRedisHost = host;
    }

    public void setApisRedisPort(int port) {
        this.instance.apisRedisPort = port;
    }

    public ApisTopologyConfig build() {
        return this.instance;
    }
}