package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.CacheDataInitializerService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Profile;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.isNull;
import static me.exrates.chartservice.configuration.CommonConfiguration.MODULE_MODE_CONSUMES;
import static me.exrates.chartservice.configuration.RedisConfiguration.NEXT_INTERVAL_MAP;

@Log4j2
@Profile(MODULE_MODE_CONSUMES)
@EnableScheduling
@Service("cacheDataInitService")
public class CacheDataInitializerServiceImpl implements CacheDataInitializerService {

    private static final BackDealInterval DEFAULT_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final RedisProcessingService redisProcessingService;
    private final TradeDataService tradeDataService;

    private Map<String, String> nextIntervalMap;

    @Autowired
    public CacheDataInitializerServiceImpl(ElasticsearchProcessingService elasticsearchProcessingService,
                                           RedisProcessingService redisProcessingService,
                                           @Qualifier(NEXT_INTERVAL_MAP) Map<String, String> nextIntervalMap, TradeDataService tradeDataService) {
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.redisProcessingService = redisProcessingService;
        this.nextIntervalMap = nextIntervalMap;
        this.tradeDataService = tradeDataService;
    }

    @PostConstruct
    public void init() {
        try {
            initializeCache();
        } catch (Exception ex) {
            log.error("--> PostConstruct 'init()' occurred error", ex);
        }
    }

    @Scheduled(cron = "${scheduled.update.cache}")
    public void updateCache() {
        try {
            initializeCache();
        } catch (Exception ex) {
            log.error("--> Job 'updateCache()' occurred error", ex);
        }
    }

    private void initializeCache() {
        log.debug("--> Start process of initialize cache <--");

        elasticsearchProcessingService.getAllIndices().parallelStream().forEach(index -> {
            boolean keyExists = redisProcessingService.exists(index, DEFAULT_INTERVAL);

            if (!keyExists) {
                List<CandleModel> models = elasticsearchProcessingService.getAllByIndex(index);
                if (!models.isEmpty()) {
                    this.initializeCache(models, index, DEFAULT_INTERVAL);
                }
            }
        });

        log.debug("--> End process of initialize cache <--");
    }

    private void initializeCache(List<CandleModel> models, String key, BackDealInterval interval) {
        if (interval != DEFAULT_INTERVAL) {
            models = CandleDataConverter.convertByInterval(models, interval);
        } else {
            List<CandleModel> finalModels = models;
            CompletableFuture.runAsync(() -> tradeDataService.defineAndSaveLastInitializedCandle(key, finalModels));
        }

        redisProcessingService.batchInsertOrUpdate(models, key, interval);

        final String nextInterval = nextIntervalMap.get(interval.getInterval());
        if (isNull(nextInterval)) {
            return;
        }
        initializeCache(models, key, new BackDealInterval(nextInterval));
    }
}
