package me.exrates.chartservice.services.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Cleanup;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static java.util.Objects.isNull;
import static me.exrates.chartservice.configuration.CommonConfiguration.JSON_MAPPER;
import static me.exrates.chartservice.configuration.RedisConfiguration.DB_INDEX_MAP;

@Log4j2
@Service
public class RedisProcessingServiceImpl implements RedisProcessingService {

    private static final String KEYS_PATTERN = "*";

    private Map<String, Integer> dbIndexMap;

    private final JedisPool jedisPool;
    private final ObjectMapper mapper;

    @Autowired
    public RedisProcessingServiceImpl(@Qualifier(DB_INDEX_MAP) Map<String, Integer> dbIndexMap,
                                      JedisPool jedisPool,
                                      @Qualifier(JSON_MAPPER) ObjectMapper mapper) {
        this.dbIndexMap = dbIndexMap;
        this.jedisPool = jedisPool;
        this.mapper = mapper;
    }

    @Override
    public List<String> getAllKeys(BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        return new ArrayList<>(jedis.keys(KEYS_PATTERN));
    }

    @Override
    public boolean exists(String key, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        return jedis.exists(key);
    }

    @Override
    public CandleModel get(String key, String hashKey, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        if (jedis.hexists(key, hashKey)) {
            try {
                return mapper.readValue(jedis.hget(key, hashKey), CandleModel.class);
            } catch (IOException ex) {
                log.error("Problem with getting response from redis", ex);
                return null;
            }
        }
        return null;
    }

    @Override
    public List<CandleModel> getAllByKey(String key, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        if (jedis.exists(key)) {
            return jedis.hgetAll(key).values().stream()
                    .map(value -> {
                        try {
                            return mapper.readValue(value, CandleModel.class);
                        } catch (IOException ex) {
                            log.error("Problem with getting response from redis", ex);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public List<CandleModel> getByRange(LocalDateTime from, LocalDateTime to, String key, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        if (jedis.exists(key)) {
            return jedis.hgetAll(key).values().stream()
                    .map(value -> {
                        try {
                            return mapper.readValue(value, CandleModel.class);
                        } catch (IOException ex) {
                            log.error("Problem with getting response from redis", ex);
                            return null;
                        }
                    })
                    .filter(Objects::nonNull)
                    .filter(model -> model.getCandleOpenTime().isAfter(from) && model.getCandleOpenTime().isBefore(to))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    @Override
    public void batchInsertOrUpdate(List<CandleModel> models, String key, BackDealInterval interval) {
        models.forEach(model -> this.insertOrUpdate(model, key, interval));
    }

    @Override
    public void insertOrUpdate(CandleModel model, String key, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        String valueString = getSourceString(model);
        if (isNull(valueString)) {
            return;
        }

        final String hashKey = RedisGeneratorUtil.generateHashKey(model.getCandleOpenTime());

        Long result = jedis.hset(key, hashKey, valueString);

        if (result == 0) {
            log.debug("Value have been updated in redis");
        } else if (result == 1) {
            log.debug("Value have been inserted into redis");
        } else {
            log.warn("Process of inserting or updating are corrupted");
        }
    }

    @Override
    public void deleteAll() {
        dbIndexMap.values().stream()
                .sorted(Comparator.naturalOrder())
                .forEach(this::deleteByDbIndex);
    }

    @Override
    public void deleteByDbIndex(int dbIndex) {
        @Cleanup Jedis jedis = getJedis(dbIndex);

        jedis.keys(KEYS_PATTERN).forEach(jedis::del);
    }

    @Override
    public void deleteByHashKey(String key, String hashKey, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        jedis.hdel(key, hashKey);
    }

    /**
     * Get Jedis instance
     */
    private Jedis getJedis(int dbIndex) {
        Jedis jedisPoolResource = jedisPool.getResource();
        jedisPoolResource.select(dbIndex);
        return jedisPoolResource;
    }

    private String getSourceString(final CandleModel model) {
        try {
            return mapper.writeValueAsString(model);
        } catch (JsonProcessingException ex) {
            log.error("Problem with writing model object to string", ex);
            return null;
        }
    }
}