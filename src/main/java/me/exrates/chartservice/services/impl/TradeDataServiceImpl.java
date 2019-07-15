package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CandlesDataDto;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static me.exrates.chartservice.configuration.CommonConfiguration.ALL_SUPPORTED_INTERVALS_LIST;
import static me.exrates.chartservice.converters.CandleDataConverter.merge;
import static me.exrates.chartservice.converters.CandleDataConverter.reduceToCandle;
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
                                @Value("${candles.store-in-cache}") long candlesToStoreInCache,
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
    public CandlesDataDto getCandles(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval) {
        if (from.isAfter(to)) {
            return null;
        }
        List<CandleModel> candleModels;

        LocalDateTime fromTime = getNearestBackTimeForBackdealInterval(from, interval);
        LocalDateTime toTime = getNearestBackTimeForBackdealInterval(to, interval);
        LocalDateTime oldestCachedCandleTime = getCandleTimeByCount(candlesToStoreInCache, interval);

        final String key = RedisGeneratorUtil.generateKey(pairName);

        if (to.isBefore(oldestCachedCandleTime)) {
            candleModels = getCandlesFromElasticAndAggregateToInterval(pairName, fromTime, toTime, interval);
        } else if (fromTime.isBefore(oldestCachedCandleTime) && to.isAfter(oldestCachedCandleTime)) {
            candleModels = Stream.of(redisProcessingService.getByRange(oldestCachedCandleTime, toTime, key, interval),
                    getCandlesFromElasticAndAggregateToInterval(pairName, fromTime, oldestCachedCandleTime.minusSeconds(1), interval))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        } else {
            candleModels = redisProcessingService.getByRange(fromTime, toTime, key, interval);
        }
        return new CandlesDataDto(candleModels, pairName, interval);
    }

    @Override
    public void handleReceivedTrades(String pairName, List<TradeDataDto> dto) {
        dto.stream()
                .collect(Collectors.groupingBy(p -> getNearestTimeBeforeForMinInterval(p.getTradeDate())))
                .forEach((k, v) -> groupTradesAndSave(pairName, v));
    }

    @Override
    public void defineAndSaveLastInitializedCandleTime(String pairName, List<CandleModel> models) {
        if (!CollectionUtils.isEmpty(models)) {
            models.stream()
                    .map(CandleModel::getCandleOpenTime)
                    .max(LocalDateTime::compareTo)
                    .ifPresent(dateTime -> redisProcessingService.insertLastInitializedCandleTimeToCache(pairName, dateTime));
        }
    }

    private void groupTradesAndSave(String pairName, List<TradeDataDto> dto) {
        xSync.execute(pairName, () -> {
            CandleModel reducedCandle = reduceToCandle(dto);
            supportedIntervals.forEach(p -> {
                CandleModel cachedCandleModel = redisProcessingService.get(pairName, RedisGeneratorUtil.generateKey(pairName), p);
                CandleModel mergedCandle = merge(cachedCandleModel, reducedCandle, p);
                redisProcessingService.insertOrUpdate(mergedCandle, pairName, p);
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

        return CandleDataConverter.convertByInterval(elasticsearchProcessingService.getByRange(from, to, index), interval);
    }
}
