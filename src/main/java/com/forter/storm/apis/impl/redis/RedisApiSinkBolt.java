package com.forter.storm.apis.impl.redis;

import org.apache.storm.task.TopologyContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.storm.apis.ApiSinkBolt;
import com.forter.storm.apis.ObjectMapperHolder;
import com.google.common.base.Throwables;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Map;

/**
 * Created by reem on 10/7/14.
 */
public class RedisApiSinkBolt extends ApiSinkBolt {
    private transient ObjectWriter writer;
    private transient JedisPool pool;
    private RedisApisConfiguration configuration;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        super.prepare(stormConf, context);
        this.configuration = (RedisApisConfiguration) apisConfiguration.getTransport();
        this.writer = ObjectMapperHolder.getWriter();

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(2);
        this.pool = new JedisPool(config, configuration.getApisRedisHost(), configuration.getApisRedisPort());
    }

    @Override
    protected void registerApiResult(final String id, final ObjectNode response) {
        Jedis resource = null;
        try {
            resource = this.pool.getResource();
            resource.publish(configuration.getApisRedisResponseChannel() + "." + id, writer.writeValueAsString(response));
        } catch (JsonProcessingException e) {
            throw Throwables.propagate(e);
        } catch (JedisConnectionException e) {
            this.pool.returnBrokenResource(resource);
            resource = null;
            throw Throwables.propagate(e);
        } finally {
            if (resource != null) {
                this.pool.returnResource(resource);
            }
        }
    }
}
