package me.exrates.chartservice.services.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Cleanup;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.DailyDataModel;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

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

    private static final String LAST = "last_cached_candle_time";
    private static final String FIRST = "first_history_candle_time";

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
    public LocalDateTime getLastCandleTimeBeforeDate(LocalDateTime candleDateTime, LocalDateTime boundaryTime, String hashKey, BackDealInterval interval) {
        return getLastCandleTimeBeforeDate(candleDateTime, boundaryTime.toLocalDate(), candleDateTime.toLocalDate(), hashKey, interval);
    }

    private LocalDateTime getLastCandleTimeBeforeDate(LocalDateTime candleDateTime, LocalDate boundaryDate, LocalDate date, String hashKey, BackDealInterval interval) {
        if (date.isBefore(boundaryDate)) {
            return null;
        }

        final String key = RedisGeneratorUtil.generateKey(date);

        List<CandleModel> models = get(key, hashKey, interval);
        if (!CollectionUtils.isEmpty(models)) {
            LocalDateTime lastCandleTime = models.stream()
                    .map(CandleModel::getCandleOpenTime)
                    .filter(candleOpenTime -> candleOpenTime.isBefore(candleDateTime))
                    .max(Comparator.naturalOrder())
                    .orElse(null);

            if (Objects.nonNull(lastCandleTime)) {
                return lastCandleTime;
            }
        }
        return getLastCandleTimeBeforeDate(candleDateTime, boundaryDate, date.minusDays(1), hashKey, interval);
    }

    @Override
    public void bulkInsertOrUpdate(Map<String, List<CandleModel>> mapOfModels, String hashKey, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        Pipeline pipeline = jedis.pipelined();

        mapOfModels.forEach((key, models) -> {
            String valueString = getSourceString(models);
            if (Objects.nonNull(valueString)) {
                pipeline.hset(key, hashKey, valueString);
            }
        });
        pipeline.sync();
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
    public void insertLastInitializedCandleTimeToCache(String key, LocalDateTime dateTime) {
        @Cleanup Jedis jedis = getJedis(0);

        jedis.hset(key, LAST, TimeUtil.generateDateTimeString(dateTime));
    }

    @Override
    public LocalDateTime getLastInitializedCandleTimeFromCache(String key) {
        @Cleanup Jedis jedis = getJedis(0);

        if (!jedis.hexists(key, LAST)) {
            return null;
        }
        return TimeUtil.generateDateTime(jedis.hget(key, LAST));
    }

    @Override
    public void insertFirstInitializedCandleTimeToHistory(String key, LocalDateTime dateTime) {
        @Cleanup Jedis jedis = getJedis(0);

        jedis.hset(key, FIRST, TimeUtil.generateDateTimeString(dateTime));
    }

    @Override
    public LocalDateTime getFirstInitializedCandleTimeFromHistory(String key) {
        @Cleanup Jedis jedis = getJedis(0);

        if (!jedis.hexists(key, FIRST)) {
            return null;
        }
        return TimeUtil.generateDateTime(jedis.hget(key, FIRST));
    }

    //methods for coinmarketcap business logic


    @Override
    public List<String> getDailyDataKeys() {
        @Cleanup Jedis jedis = getJedis(15);

        return new ArrayList<>(jedis.keys(ALL));
    }

    @Override
    public void insertDailyData(DailyDataModel dataModel, String key, String hashKey) {
        @Cleanup Jedis jedis = getJedis(15);

        String valueString = getSourceString(dataModel);
        if (isNull(valueString)) {
            return;
        }

        Long result = jedis.hset(key, hashKey, valueString);

        if (result != 0 && result != 1) {
            log.warn("Process of inserting or updating are corrupted");
        }
    }

    @Override
    public DailyDataModel getDailyData(String key, String hashKey) {
        @Cleanup Jedis jedis = getJedis(15);

        if (!jedis.hexists(key, hashKey)) {
            return null;
        }
        return getDailyDataModel(jedis.hget(key, hashKey));
    }

    @Override
    public List<DailyDataModel> getDailyDataByKey(String key) {
        @Cleanup Jedis jedis = getJedis(15);

        if (!jedis.exists(key)) {
            return Collections.emptyList();
        }
        return jedis.hgetAll(key).values().stream()
                .map(this::getDailyDataModel)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public void deleteAllDailyDataKeys() {
        @Cleanup Jedis jedis = getJedis(15);

        jedis.del(ALL);
    }

    @Override
    public void deleteDailyData(String key, String hashKey) {
        @Cleanup Jedis jedis = getJedis(15);

        jedis.hdel(key, hashKey);
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

    private String getSourceString(final List<CandleModel> models) {
        try {
            return mapper.writeValueAsString(models);
        } catch (JsonProcessingException ex) {
            log.error("Problem with writing model object into string", ex);

            return null;
        }
    }

    private String getSourceString(final DailyDataModel dataModel) {
        try {
            return mapper.writeValueAsString(dataModel);
        } catch (JsonProcessingException ex) {
            log.error("Problem with writing daily data model object into string", ex);

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

    private DailyDataModel getDailyDataModel(final String sourceString) {
        try {
            return mapper.readValue(sourceString, DailyDataModel.class);
        } catch (IOException ex) {
            log.error("Problem with getting response from redis", ex);

            return null;
        }
    }
}