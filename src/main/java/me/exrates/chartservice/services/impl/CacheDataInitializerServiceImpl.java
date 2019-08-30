package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.CacheDataInitializerService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.isNull;
import static me.exrates.chartservice.configuration.CommonConfiguration.ALL_SUPPORTED_INTERVALS_LIST;
import static me.exrates.chartservice.configuration.CommonConfiguration.DEFAULT_INTERVAL;
import static me.exrates.chartservice.configuration.RedisConfiguration.NEXT_INTERVAL_MAP;

@Log4j2
@EnableScheduling
@Service("cacheDataInitService")
public class CacheDataInitializerServiceImpl implements CacheDataInitializerService {

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final RedisProcessingService redisProcessingService;
    private final TradeDataService tradeDataService;

    private long candlesToStoreInCache;

    private List<BackDealInterval> supportedIntervals;
    private Map<String, String> nextIntervalMap;

    @Autowired
    public CacheDataInitializerServiceImpl(ElasticsearchProcessingService elasticsearchProcessingService,
                                           RedisProcessingService redisProcessingService,
                                           TradeDataService tradeDataService,
                                           @Value("${candles.store-in-cache:300}") long candlesToStoreInCache,
                                           @Qualifier(ALL_SUPPORTED_INTERVALS_LIST) List<BackDealInterval> supportedIntervals,
                                           @Qualifier(NEXT_INTERVAL_MAP) Map<String, String> nextIntervalMap) {
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.redisProcessingService = redisProcessingService;
        this.tradeDataService = tradeDataService;
        this.candlesToStoreInCache = candlesToStoreInCache;
        this.supportedIntervals = supportedIntervals;
        this.nextIntervalMap = nextIntervalMap;
    }

    @PostConstruct
    public void init() {
        try {
            updateCache();
        } catch (Exception ex) {
            log.error("--> PostConstruct 'init()' occurred error", ex);
        }
    }

    @Override
    public void updateCache() {
        log.info("--> Start process of update cache <--");

        elasticsearchProcessingService.getAllIndices().parallelStream().forEach(this::updateCacheByKey);

        log.info("--> End process of update cache <--");
    }

    @Override
    public void updateCacheByKey(String key) {
        this.updateCache(key, DEFAULT_INTERVAL);
    }

    @Override
    public void updateCache(String key, BackDealInterval interval) {
        final LocalDateTime fromDate = getBoundaryTime(interval);
        final LocalDateTime toDate = TimeUtil.getNearestBackTimeForBackdealInterval(LocalDateTime.now(), interval).plusDays(1);

        List<CandleModel> models = elasticsearchProcessingService.getByRange(fromDate, toDate, key);

        if (!CollectionUtils.isEmpty(models)) {
            if (!Objects.equals(interval, DEFAULT_INTERVAL)) {
                models = CandleDataConverter.convertByInterval(models, interval);
            } else {
                List<CandleModel> finalModels = models;
                CompletableFuture.runAsync(() -> tradeDataService.defineAndSaveLastInitializedCandleTime(key, finalModels));
            }
            models.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

            models.forEach(model -> {
                final String hashKey = RedisGeneratorUtil.generateHashKey(model.getCandleOpenTime());

                CandleModel cachedModel = redisProcessingService.get(key, hashKey, interval);

                if (isNull(cachedModel)) {
                    redisProcessingService.insertOrUpdate(model, key, interval);
                }
            });
        }

        final String nextInterval = nextIntervalMap.get(interval.getInterval());
        if (isNull(nextInterval)) {
            return;
        }
        updateCache(key, new BackDealInterval(nextInterval));
    }

    @Override
    public void cleanCache() {
        log.info("--> Start process of clean cache <--");

        supportedIntervals.parallelStream().forEach(this::cleanCache);

        log.info("--> End process of clean cache <--");
    }

    @Override
    public void cleanCache(BackDealInterval interval) {
        redisProcessingService.getAllKeys(interval).forEach(key -> {
            redisProcessingService.getAllByKey(key, interval).stream()
                    .filter(model -> getBoundaryTime(interval).isAfter(model.getCandleOpenTime()))
                    .map(model -> Pair.of(RedisGeneratorUtil.generateHashKey(model.getCandleOpenTime()), model))
                    .forEach(pair -> {
                        final String hashKey = pair.getKey();
                        final CandleModel model = pair.getValue();

                        if (Objects.equals(interval, DEFAULT_INTERVAL)) {
                            if (elasticsearchProcessingService.exists(key, hashKey)) {
                                CandleModel savedModel = elasticsearchProcessingService.get(key, hashKey);

                                if (Objects.nonNull(savedModel) && model.getLastTradeTime().isAfter(savedModel.getLastTradeTime())) {
                                    elasticsearchProcessingService.update(model, key);
                                }
                            } else {
                                elasticsearchProcessingService.insert(model, key);
                            }
                        }
                        redisProcessingService.deleteDataByHashKey(key, hashKey, interval);
                    });

            if (Objects.equals(interval, DEFAULT_INTERVAL)) {
                List<CandleModel> allByKey = redisProcessingService.getAllByKey(key, DEFAULT_INTERVAL);
                CompletableFuture.runAsync(() -> tradeDataService.defineAndSaveLastInitializedCandleTime(key, allByKey));
            }
        });
    }

    private LocalDateTime getBoundaryTime(BackDealInterval interval) {
        LocalDateTime currentCandleTime = TimeUtil.getNearestBackTimeForBackdealInterval(LocalDateTime.now(), interval);

        return currentCandleTime.minusMinutes(candlesToStoreInCache * TimeUtil.convertToMinutes(interval));
    }
}