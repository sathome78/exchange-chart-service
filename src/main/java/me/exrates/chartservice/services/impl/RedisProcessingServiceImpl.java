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

    private static final String ALL = "*";

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

        return new ArrayList<>(jedis.keys(ALL));
    }

    @Override
    public boolean exists(String key, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        return jedis.exists(key);
    }

    @Override
    public boolean exists(String key, int dbIndex) {
        @Cleanup Jedis jedis = getJedis(dbIndex);

        return jedis.exists(key);
    }

    @Override
    public CandleModel get(String key, String hashKey, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        if (!jedis.hexists(key, hashKey)) {
            return null;
        }
        return getModel(jedis.hget(key, hashKey));
    }

    @Override
    public List<CandleModel> getAllByKey(String key, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        if (!jedis.exists(key)) {
            return Collections.emptyList();
        }
        return jedis.hgetAll(key).values().stream()
                .map(this::getModel)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public List<CandleModel> getByRange(LocalDateTime from, LocalDateTime to, String key, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        if (!jedis.exists(key)) {
            return Collections.emptyList();
        }
        return jedis.hgetAll(key).values().stream()
                .map(this::getModel)
                .filter(Objects::nonNull)
                .filter(model -> (model.getCandleOpenTime().isEqual(from) || model.getCandleOpenTime().isAfter(from))
                        && (model.getCandleOpenTime().isEqual(to) || model.getCandleOpenTime().isBefore(to)))
                .collect(Collectors.toList());
    }

    @Override
    public LocalDateTime getLastCandleTimeBeforeDate(LocalDateTime date, String key, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        if (!jedis.exists(key)) {
            return null;
        }
        return jedis.hgetAll(key).values().stream()
                .map(this::getModel)
                .filter(Objects::nonNull)
                .map(CandleModel::getCandleOpenTime)
                .filter(candleOpenTime -> candleOpenTime.isBefore(date))
                .max(LocalDateTime::compareTo)
                .orElse(null);
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
    public void deleteAllKeys() {
        dbIndexMap.values().stream()
                .sorted(Comparator.naturalOrder())
                .forEach(dbIndex -> deleteKeyByDbIndexAndKey(dbIndex, ALL));
    }

    @Override
    public void deleteKey(String key) {
        dbIndexMap.values().stream()
                .sorted(Comparator.naturalOrder())
                .forEach(dbIndex -> deleteKeyByDbIndexAndKey(dbIndex, key));
    }

    @Override
    public void deleteKeyByDbIndexAndKey(int dbIndex, String key) {
        @Cleanup Jedis jedis = getJedis(dbIndex);

        if (ALL.equals(key)) {
            jedis.keys(ALL).forEach(jedis::del);
        } else {
            jedis.del(key);
        }
    }

    @Override
    public void deleteDataByHashKey(String key, String hashKey, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        jedis.hdel(key, hashKey);
    }

    @Override
    public void insertLastInitializedCandleTimeToCache(String key, LocalDateTime dateTime) {
        @Cleanup Jedis jedis = getJedis(0);

        jedis.set(key, RedisGeneratorUtil.generateHashKey(dateTime));
    }

    @Override
    public LocalDateTime getLastInitializedCandleTimeFromCache(String key) {
        @Cleanup Jedis jedis = getJedis(0);

        return RedisGeneratorUtil.generateDateTime(jedis.get(key));
    }

    @Override
    public Long publishMessage(String channel, String message) {
        @Cleanup Jedis jedis = getJedis(0);
        return jedis.publish(channel, message);
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
            log.error("Problem with writing model object into string", ex);

            return null;
        }
    }

    private CandleModel getModel(final String sourceString) {
        try {
            return mapper.readValue(sourceString, CandleModel.class);
        } catch (IOException ex) {
            log.error("Problem with getting response from redis", ex);

            return null;
        }
    }
}
