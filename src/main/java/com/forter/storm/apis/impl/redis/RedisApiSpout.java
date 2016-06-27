package com.forter.storm.apis.impl.redis;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.forter.storm.apis.ApiRequestSpout;
import com.forter.storm.apis.ApisTopologyConfig;
import com.forter.storm.apis.ObjectMapperHolder;
import com.google.common.base.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.Map;

/**
 * Created by reem on 10/7/14.
 */
public abstract class RedisApiSpout<C extends ApisTopologyConfig, T extends RedisApisConfiguration>
        extends ApiRequestSpout<C> {
    private static Logger logger = LoggerFactory.getLogger(RedisApiSpout.class);

    protected final T transportConfig;

    private transient JedisPool pool;
    private transient ObjectWriter writer;

    public RedisApiSpout(C config) {
        super(config);
        this.transportConfig = (T) config.getTransport();
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        super.open(conf, context, collector);
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(2);
        this.pool = new JedisPool(config, transportConfig.getApisRedisHost(), transportConfig.getApisRedisPort());
        this.writer = ObjectMapperHolder.getWriter();
    }

    protected void reportError(String id, ObjectNode error) {
        Jedis resource = null;
        try {
            resource = this.pool.getResource();
            resource.lpush(transportConfig.getApisRedisResponseChannel(), writer.writeValueAsString(error));
        } catch (JsonProcessingException e) {
            logger.warn("Error while reporting error", e);
        } catch (JedisConnectionException e) {
            this.pool.returnBrokenResource(resource);
            resource = null;
        } finally {
            if (resource != null) {
                this.pool.returnResource(resource);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        this.pool.close();
    }

    @Override
    protected String getApiCommandJson() {
        Jedis resource = null;
        try {
            resource = this.pool.getResource();
            return resource.rpop(transportConfig.getApisRedisRequestQueue());
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
