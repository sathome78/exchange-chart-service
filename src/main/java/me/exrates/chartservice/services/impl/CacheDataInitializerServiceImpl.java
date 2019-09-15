package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.ModelList;
import me.exrates.chartservice.services.CacheDataInitializerService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

import static me.exrates.chartservice.configuration.CommonConfiguration.ALL_SUPPORTED_INTERVALS_LIST;
import static me.exrates.chartservice.configuration.CommonConfiguration.DEFAULT_INTERVAL;

@Log4j2
@EnableScheduling
@Service("cacheDataInitService")
public class CacheDataInitializerServiceImpl implements CacheDataInitializerService {

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final RedisProcessingService redisProcessingService;
    private final TradeDataService tradeDataService;

    private long candlesToStoreInCache;

    private List<BackDealInterval> supportedIntervals;

    @Autowired
    public CacheDataInitializerServiceImpl(ElasticsearchProcessingService elasticsearchProcessingService,
                                           RedisProcessingService redisProcessingService,
                                           TradeDataService tradeDataService,
                                           @Value("${candles.store-in-cache:300}") long candlesToStoreInCache,
                                           @Qualifier(ALL_SUPPORTED_INTERVALS_LIST) List<BackDealInterval> supportedIntervals) {
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.redisProcessingService = redisProcessingService;
        this.tradeDataService = tradeDataService;
        this.candlesToStoreInCache = candlesToStoreInCache;
        this.supportedIntervals = supportedIntervals;
    }

//    @PostConstruct
//    public void init() {
//        try {
//            updateCache();
//        } catch (Exception ex) {
//            log.error("--> PostConstruct 'init()' occurred error", ex);
//        }
//    }

    @Override
    public void updateCache() {
        log.info("--> Start process of update cache <--");

        elasticsearchProcessingService.getAllIndices().forEach(this::updateCacheByIndex);

        log.info("--> End process of update cache <--");
    }

    @Override
    public void updateCacheByIndex(String index) {
        Map<String, List<CandleModel>> allByIndex = elasticsearchProcessingService.getAllByIndex(index);

        allByIndex.forEach((id, models) -> {
            if (!CollectionUtils.isEmpty(models)) {
                supportedIntervals.parallelStream().forEach(interval -> updateCache(models, index, id, interval));
            }
        });
    }

    @Override
    public void updateCacheByIndexAndId(String index, String id) {
        List<CandleModel> models = elasticsearchProcessingService.get(index, id);
        if (!CollectionUtils.isEmpty(models)) {
            supportedIntervals.parallelStream().forEach(interval -> updateCache(models, index, id, interval));
        }
    }

    @Override
    public void updateCache(List<CandleModel> models, String key, String hashKey, BackDealInterval interval) {
        if (!Objects.equals(interval, DEFAULT_INTERVAL)) {
            models = CandleDataConverter.convertByInterval(models, interval);
        } else {
            List<CandleModel> finalModels = models;
            CompletableFuture.runAsync(() -> tradeDataService.defineAndSaveLastInitializedCandleTime(hashKey, finalModels));
        }
        models.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

        final LocalDate keyDate = TimeUtil.generateDate(key);
        final LocalDate boundaryTime = getBoundaryTime(interval);

        if (!redisProcessingService.exists(key, hashKey, interval)
                && Objects.nonNull(keyDate)
                && (boundaryTime.isEqual(keyDate) || boundaryTime.isBefore(keyDate))) {
            redisProcessingService.insertOrUpdate(models, key, hashKey, interval);
        }
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
            final LocalDate keyDate = TimeUtil.generateDate(key);

            if (Objects.nonNull(keyDate) && getBoundaryTime(interval).isAfter(keyDate)) {
                redisProcessingService.getAllByKey(key, interval).forEach((hashKey, model) -> {
                    if (Objects.equals(interval, DEFAULT_INTERVAL)) {
                        if (elasticsearchProcessingService.exists(key, hashKey)) {
                            elasticsearchProcessingService.update(model, key, hashKey);
                        } else {
                            elasticsearchProcessingService.insert(model, key, hashKey);
                        }
                    }
                    redisProcessingService.deleteDataByHashKey(key, hashKey, interval);
                });

                if (Objects.equals(interval, DEFAULT_INTERVAL)) {
                    Map<String, List<CandleModel>> mapOfModels = redisProcessingService.getAllByKey(key, interval);

                    mapOfModels.forEach(tradeDataService::defineAndSaveLastInitializedCandleTime);
                }
            }
        });
    }

    private LocalDate getBoundaryTime(BackDealInterval interval) {
        LocalDateTime currentDateTime = LocalDateTime.now();

        return currentDateTime.minusMinutes(candlesToStoreInCache * TimeUtil.convertToMinutes(interval)).toLocalDate();
    }
}