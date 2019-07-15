package me.exrates.chartservice.configuration;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import redis.clients.jedis.JedisPool;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

@Configuration
public class RedisConfiguration {

    public static final String DB_INDEX_MAP = "dbIndexMap";
    public static final String NEXT_INTERVAL_MAP = "nextIntervalMap";

    @Value("${redis.host}")
    private String host;
    @Value("${redis.port}")
    private int port;
    @Value("${redis.allowed-interval-db-indexes}")
    private String allowedIntervalDbIndexesString;
    @Value("${redis.next-interval}")
    private String nextIntervalString;

    @Bean
    JedisPool jedisPool() {
        return new JedisPool(host, port);
    }

    @Bean(DB_INDEX_MAP)
    public Map<String, Integer> prepareDbIndexMap() {
        return Arrays.stream(allowedIntervalDbIndexesString.split(";"))
                .map(row -> row.split(":"))
                .filter(row -> row.length > 1)
                .collect(Collectors.toMap(
                        key -> key[0].trim(),
                        row -> Integer.parseInt(row[1].trim())));
    }

    @Bean(NEXT_INTERVAL_MAP)
    public Map<String, String> prepareNextIntervalMap() {
        return Arrays.stream(nextIntervalString.split(";"))
                .map(row -> row.split(":"))
                .filter(row -> row.length > 1)
                .collect(Collectors.toMap(
                        key -> key[0].trim(),
                        row -> row[1].trim()));
    }
}