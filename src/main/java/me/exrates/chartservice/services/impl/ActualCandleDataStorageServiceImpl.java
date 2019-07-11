package me.exrates.chartservice.services.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.Cleanup;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.ActualCandleDataStorageService;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
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
public class ActualCandleDataStorageServiceImpl implements ActualCandleDataStorageService {

    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("dd_MM_yyyy_HH_mm");

    private static final String KEYS_PATTERN = "*";

    private Map<String, Integer> dbIndexMap;

    private final JedisPool jedisPool;
    private final ObjectMapper mapper;

    @Autowired
    public ActualCandleDataStorageServiceImpl(@Qualifier(DB_INDEX_MAP) Map<String, Integer> dbIndexMap,
                                              JedisPool jedisPool,
                                              @Qualifier(JSON_MAPPER) ObjectMapper mapper) {
        this.dbIndexMap = dbIndexMap;
        this.jedisPool = jedisPool;
        this.mapper = mapper;
    }

    @Override
    public CandleModel get(String pairName, LocalDateTime dateTime, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        final String key = prepareKey(pairName);
        final String hashKey = prepareHashKey(dateTime);

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
    public List<CandleModel> getByRange(LocalDateTime from, LocalDateTime to, String pairName, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        final String key = prepareKey(pairName);

        if (jedis.exists(key)) {
            Map<String, String> valuesMap = jedis.hgetAll(key);

            return valuesMap.entrySet().stream()
                    .map(entry -> Pair.of(LocalDateTime.parse(entry.getKey(), FORMATTER), entry.getValue()))
                    .filter(pair -> pair.getKey().isAfter(from) && pair.getKey().isBefore(to))
                    .map(Pair::getValue)
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
    public void batchInsertOrUpdate(List<CandleModel> models, String pairName, BackDealInterval interval) {
        models.forEach(model -> this.insertOrUpdate(model, pairName, interval));
    }

    @Override
    public void insertOrUpdate(CandleModel model, String pairName, BackDealInterval interval) {
        @Cleanup Jedis jedis = getJedis(dbIndexMap.get(interval.getInterval()));

        String valueString = getSourceString(model);
        if (isNull(valueString)) {
            return;
        }

        final String key = prepareKey(pairName);
        final String hashKey = prepareHashKey(model.getCandleOpenTime());

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
                .forEach(this::deleteByIndex);
    }

    @Override
    public void deleteByIndex(int index) {
        @Cleanup Jedis jedis = getJedis(index);

        jedis.keys(KEYS_PATTERN).forEach(jedis::del);
    }

    /**
     * Get Jedis instance
     */
    private Jedis getJedis(int index) {
        Jedis jedisPoolResource = jedisPool.getResource();
        jedisPoolResource.select(index);
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

    private String prepareKey(String pairName) {
        return pairName.replace("/", "_").toLowerCase();
    }

    private String prepareHashKey(LocalDateTime dateTime) {
        return dateTime.format(FORMATTER);
    }
}