package me.exrates.chartservice.services.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Cleanup;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.ModelList;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.time.LocalDate;
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
    public boolean exists(String key, String hashKey, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        return jedis.hexists(key, hashKey);
    }

    @Override
    public List<CandleModel> get(String key, String hashKey, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        if (!exists(key, hashKey, interval)) {
            return null;
        }
        return getModels(jedis.hget(key, hashKey));
    }

    @Override
    public Map<String, List<CandleModel>> getAllByKey(String key, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        if (!jedis.exists(key)) {
            return Collections.emptyMap();
        }
        return jedis.hgetAll(key).entrySet().stream()
                .map(entry -> Pair.of(entry.getKey(), getModels(entry.getValue())))
                .filter(pair -> Objects.nonNull(pair.getValue()))
                .collect(Collectors.toMap(
                        Pair::getKey,
                        Pair::getValue));
    }

    @Override
    public LocalDateTime getLastCandleTimeBeforeDate(LocalDateTime candleDateTime, String hashKey, BackDealInterval interval) {
        return getLastCandleTimeBeforeDate(candleDateTime, candleDateTime.toLocalDate(), hashKey, interval);
    }

    private LocalDateTime getLastCandleTimeBeforeDate(LocalDateTime candleDateTime, LocalDate date, String hashKey, BackDealInterval interval) {
        final String key = RedisGeneratorUtil.generateKey(date);

        List<CandleModel> models = get(key, hashKey, interval);
        if (!CollectionUtils.isEmpty(models)) {
            return models.stream()
                    .map(CandleModel::getCandleOpenTime)
                    .filter(candleOpenTime -> candleOpenTime.isBefore(candleDateTime))
                    .max(Comparator.naturalOrder())
                    .orElse(null);
        }
        return getLastCandleTimeBeforeDate(candleDateTime, date.minusDays(1), hashKey, interval);
    }

    @Override
    public void bulkInsertOrUpdate(Map<String, List<CandleModel>> mapOfModels, String key, BackDealInterval interval) {
        mapOfModels.forEach((hashKey, models) -> insertOrUpdate(models, key, hashKey, interval));
    }

    @Override
    public void insertOrUpdate(List<CandleModel> models, String key, String hashKey, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        String valueString = getSourceString(models);
        if (isNull(valueString)) {
            return;
        }

        Long result = jedis.hset(key, hashKey, valueString);

        if (result != 0 && result != 1) {
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
    public void insertLastInitializedCandleTimeToCache(String hashKey, LocalDateTime dateTime) {
        @Cleanup Jedis jedis = getJedis(0);

        jedis.set(hashKey, TimeUtil.generateDateTimeString(dateTime));
    }

    @Override
    public LocalDateTime getLastInitializedCandleTimeFromCache(String hashKey) {
        @Cleanup Jedis jedis = getJedis(0);

        return TimeUtil.generateDateTime(jedis.get(hashKey));
    }

    /**
     * Get Jedis instance
     */
    private Jedis getJedis(int dbIndex) {
        Jedis jedisPoolResource = jedisPool.getResource();
        jedisPoolResource.select(dbIndex);
        return jedisPoolResource;
    }

    private String getSourceString(final List<CandleModel> models) {
        try {
            return mapper.writeValueAsString(models);
        } catch (JsonProcessingException ex) {
            log.error("Problem with writing model object into string", ex);

            return null;
        }
    }

    private List<CandleModel> getModels(final String sourceString) {
        try {
            return mapper.readValue(sourceString, new TypeReference<List<CandleModel>>() {
            });
        } catch (IOException ex) {
            log.error("Problem with getting response from redis", ex);

            return null;
        }
    }
}