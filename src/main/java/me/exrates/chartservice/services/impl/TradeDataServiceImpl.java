package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static me.exrates.chartservice.configuration.CommonConfiguration.ALL_SUPPORTED_INTERVALS_LIST;
import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;
import static me.exrates.chartservice.utils.TimeUtil.getNearestTimeBeforeForMinInterval;

@Log4j2
@Service
public class TradeDataServiceImpl implements TradeDataService {

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final RedisProcessingService redisProcessingService;
    private final XSync<String> xSync;

    private long candlesToStoreInCache;

    private List<BackDealInterval> supportedIntervals;

    @Autowired
    public TradeDataServiceImpl(ElasticsearchProcessingService elasticsearchProcessingService,
                                RedisProcessingService redisProcessingService,
                                XSync<String> xSync,
                                @Value("${candles.store-in-cache:300}") long candlesToStoreInCache,
                                @Qualifier(ALL_SUPPORTED_INTERVALS_LIST) List<BackDealInterval> supportedIntervals) {
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.redisProcessingService = redisProcessingService;
        this.xSync = xSync;
        this.candlesToStoreInCache = candlesToStoreInCache;
        this.supportedIntervals = supportedIntervals;
    }

    @Override
    public CandleModel getCandleForCurrentTime(String pairName, BackDealInterval interval) {
        return getCandle(pairName, LocalDateTime.now(), interval);
    }

    private CandleModel getCandle(String pairName, LocalDateTime dateTime, BackDealInterval interval) {
        LocalDateTime candleTime = getNearestBackTimeForBackdealInterval(dateTime, interval);

        final String key = RedisGeneratorUtil.generateKey(pairName);
        final String hashKey = RedisGeneratorUtil.generateHashKey(candleTime);

        return redisProcessingService.get(key, hashKey, interval);
    }

    @Override
    public List<CandleModel> getCandles(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval) {
        if (isNull(from) || isNull(to) || from.isAfter(to)) {
            return Collections.emptyList();
        }

        from = getNearestBackTimeForBackdealInterval(from, interval);
        to = getNearestBackTimeForBackdealInterval(to, interval);

        final LocalDateTime oldestCachedCandleTime = getCandleTimeByCount(candlesToStoreInCache, interval);
        final String key = RedisGeneratorUtil.generateKey(pairName);

        List<CandleModel> candleModels;
        if (to.isBefore(oldestCachedCandleTime)) {
            candleModels = getCandlesFromElasticAndAggregateToInterval(pairName, from, to, interval);
        } else if (from.isBefore(oldestCachedCandleTime) && to.isAfter(oldestCachedCandleTime)) {
            candleModels = Stream.of(redisProcessingService.getByRange(oldestCachedCandleTime, to, key, interval),
                    getCandlesFromElasticAndAggregateToInterval(pairName, from, oldestCachedCandleTime.minusSeconds(1), interval))
                    .flatMap(Collection::stream)
                    .distinct()
                    .collect(Collectors.toList());
        } else {
            candleModels = redisProcessingService.getByRange(from, to, key, interval);
        }

        CandleDataConverter.fixOpenRate(candleModels);

        return candleModels;
    }

    @Override
    public LocalDateTime getLastCandleTimeBeforeDate(String pairName, LocalDateTime date, BackDealInterval interval) {
        if (isNull(date)) {
            return null;
        }

        date = getNearestBackTimeForBackdealInterval(date, interval);

        final LocalDateTime oldestCachedCandleTime = getCandleTimeByCount(candlesToStoreInCache, interval);
        final String key = RedisGeneratorUtil.generateKey(pairName);

        if (date.isAfter(oldestCachedCandleTime)) {
            return redisProcessingService.getLastCandleTimeBeforeDate(date, key, interval);
        } else {
            return elasticsearchProcessingService.getLastCandleTimeBeforeDate(date, key);
        }
    }

    @Override
    public void handleReceivedTrades(String pairName, List<TradeDataDto> dto) {
        dto.stream()
                .collect(Collectors.groupingBy(p -> getNearestTimeBeforeForMinInterval(p.getTradeDate())))
                .forEach((key, value) -> groupTradesAndSave(pairName, value));
    }

    @Override
    public void defineAndSaveLastInitializedCandleTime(String key, List<CandleModel> models) {
        if (!CollectionUtils.isEmpty(models)) {
            models.stream()
                    .map(CandleModel::getCandleOpenTime)
                    .max(LocalDateTime::compareTo)
                    .ifPresent(dateTime -> redisProcessingService.insertLastInitializedCandleTimeToCache(key, dateTime));
        }
    }

    private void groupTradesAndSave(String pairName, List<TradeDataDto> dto) {
        xSync.execute(pairName, () -> {
            CandleModel newCandle = CandleDataConverter.reduceToCandle(dto);
            if (isNull(newCandle)) {
                return;
            }

            supportedIntervals.forEach(interval -> {
                LocalDateTime candleTime = TimeUtil.getNearestBackTimeForBackdealInterval(newCandle.getCandleOpenTime(), interval);

                newCandle.setCandleOpenTime(candleTime);

                final String key = RedisGeneratorUtil.generateKey(pairName);
                final String hashKey = RedisGeneratorUtil.generateHashKey(candleTime);

                CandleModel cachedCandleModel = redisProcessingService.get(key, hashKey, interval);
                CandleModel mergedCandle = CandleDataConverter.merge(cachedCandleModel, newCandle);
                redisProcessingService.insertOrUpdate(mergedCandle, key, interval);
            });
        });
    }

    private LocalDateTime getCandleTimeByCount(long count, BackDealInterval interval) {
        LocalDateTime timeForLastCandle = getNearestBackTimeForBackdealInterval(LocalDateTime.now(), interval);
        long candlesFromBeginInterval = interval.getIntervalValue() * count;

        return timeForLastCandle.minus(candlesFromBeginInterval, interval.getIntervalType().getCorrespondingTimeUnit());
    }

    private List<CandleModel> getCandlesFromElasticAndAggregateToInterval(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval) {
        final String index = ElasticsearchGeneratorUtil.generateIndex(pairName);

        try {
            return CandleDataConverter.convertByInterval(elasticsearchProcessingService.getByRange(from, to, index), interval);
        } catch (Exception e) {
            log.error(e);
            return Collections.emptyList();
        }
    }
}