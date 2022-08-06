package com._4paradigm.csp.operator.factory;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisSinkFactory {


    public static class RedisExampleMapper implements RedisMapper<Tuple2<String, String>> {

        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "HASH_NAME");
        }

        @Override
        public String getKeyFromData(Tuple2<String, String> data) {
            return data.f0;
        }

        @Override
        public String getValueFromData(Tuple2<String, String> data) {
            return data.f1;
        }
    }

    public static class FlinkDemoRedisSink<T> extends RichSinkFunction<Tuple2<T, Long>> {
        private Jedis jedis;
        @Override
        public void open(Configuration parameters) throws Exception {
            //
            super.open(parameters);
            JedisPoolConfig jedisPoolConfig =new JedisPoolConfig();
            jedisPoolConfig.setMaxTotal(50);
            JedisPool jedisPool = new JedisPool(jedisPoolConfig, "127.0.0.1", 6379, 2000);
            jedis = jedisPool.getResource();
        }

        @Override
        public void invoke(Tuple2<T, Long> data, Context context) throws Exception {
            try {
                T param = data.f0;
                String key = "test";
                jedis.incrBy(key, data.f1);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws Exception {
            super.close();
            if (jedis != null) {
                jedis.close();
            }
        }
    }


}
