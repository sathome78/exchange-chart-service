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

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;
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

        List<CandleModel> models;
        if (to.isBefore(oldestCachedCandleTime)) {
            models = getCandlesFromElasticAndAggregateToInterval(pairName, from, to, interval);
        } else if (from.isBefore(oldestCachedCandleTime) && to.isAfter(oldestCachedCandleTime)) {
            models = Stream.of(redisProcessingService.getByRange(oldestCachedCandleTime, to, key, interval),
                    getCandlesFromElasticAndAggregateToInterval(pairName, from, oldestCachedCandleTime.minusSeconds(1), interval))
                    .flatMap(Collection::stream)
                    .distinct()
                    .collect(Collectors.toList());
        } else {
            models = redisProcessingService.getByRange(from, to, key, interval);
        }

        CandleDataConverter.fixOpenRate(models);

        return fillGaps(models, pairName, from, to, interval);
    }

    private List<CandleModel> fillGaps(List<CandleModel> models, String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval) {
        List<CandleModel> bufferedModels = new ArrayList<>(models);
        final int minutes = TimeUtil.convertToMinutes(interval);

        CandleModel initialCandle;

        if (CollectionUtils.isEmpty(bufferedModels)) {
            CandleModel previousCandle = getPreviousCandle(pairName, from, interval);

            initialCandle = nonNull(previousCandle) ? previousCandle : CandleModel.empty(BigDecimal.ZERO, null);

            while (from.isBefore(to)) {
                bufferedModels.add(CandleModel.empty(initialCandle.getCloseRate(), from));

                from = from.plusMinutes(minutes);
            }
            bufferedModels.add(CandleModel.empty(initialCandle.getCloseRate(), to));
        } else {
            bufferedModels.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

            final Map<LocalDateTime, CandleModel> modelsMap = bufferedModels.stream()
                    .collect(Collectors.toMap(CandleModel::getCandleOpenTime, Function.identity()));

            initialCandle = bufferedModels.get(0);
            if (from.isBefore(initialCandle.getCandleOpenTime())) {
                CandleModel previousCandle = getPreviousCandle(pairName, from, interval);

                initialCandle = nonNull(previousCandle) ? previousCandle : CandleModel.empty(BigDecimal.ZERO, null);
            }

            while (from.isBefore(to)) {
                CandleModel model = modelsMap.get(from);

                if (isNull(model)) {
                    bufferedModels.add(CandleModel.empty(initialCandle.getCloseRate(), from));
                } else {
                    initialCandle = model;
                }
                from = from.plusMinutes(minutes);
            }
            if (isNull(modelsMap.get(to))) {
                bufferedModels.add(CandleModel.empty(initialCandle.getCloseRate(), to));
            }
        }
        bufferedModels.sort(Comparator.comparing(CandleModel::getCandleOpenTime));

        return bufferedModels;
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
            log.debug("<<< groupTradesAndSave() >>> start");
            CandleModel newCandle = CandleDataConverter.reduceToCandle(dto);
            if (isNull(newCandle)) {
                return;
            }

            supportedIntervals.forEach(interval -> {
                log.debug("<<< groupTradesAndSave() >>> interval: {}", interval.getInterval());
                final LocalDateTime candleTime = TimeUtil.getNearestBackTimeForBackdealInterval(newCandle.getCandleOpenTime(), interval);
                newCandle.setCandleOpenTime(candleTime);
                log.debug("<<< groupTradesAndSave() >>> new candle: {}", newCandle.toString());

                final CandleModel previousCandle = getPreviousCandle(pairName, candleTime, interval);
                log.debug("<<< groupTradesAndSave() >>> previous candle: {}", previousCandle.toString());
                newCandle.setOpenRate(previousCandle.getCloseRate());
                log.debug("<<< groupTradesAndSave() >>> new candle: {}", newCandle.toString());

                final String key = RedisGeneratorUtil.generateKey(pairName);
                final String hashKey = RedisGeneratorUtil.generateHashKey(candleTime);

                CandleModel cachedCandleModel = redisProcessingService.get(key, hashKey, interval);
                CandleModel mergedCandle = CandleDataConverter.merge(cachedCandleModel, newCandle);
                log.debug("<<< groupTradesAndSave() >>> merged candle: {}", mergedCandle.toString());
                redisProcessingService.insertOrUpdate(mergedCandle, key, interval);
            });
        });
    }

    private CandleModel getPreviousCandle(String pairName, LocalDateTime candleTime, BackDealInterval interval) {
        final String key = RedisGeneratorUtil.generateKey(pairName);
        final LocalDateTime defaultPreviousCandleTime = candleTime.minusMinutes(TimeUtil.convertToMinutes(interval));

        final LocalDateTime oldestCachedCandleTime = getCandleTimeByCount(candlesToStoreInCache, interval);

        CandleModel previousModel;
        if (candleTime.isAfter(oldestCachedCandleTime)) {
            LocalDateTime lastCandleTimeBeforeDate = redisProcessingService.getLastCandleTimeBeforeDate(candleTime, key, interval);
            if (isNull(lastCandleTimeBeforeDate)) {
                return CandleModel.empty(BigDecimal.ZERO, defaultPreviousCandleTime);
            }

            final String hashKey = RedisGeneratorUtil.generateHashKey(lastCandleTimeBeforeDate);

            previousModel = redisProcessingService.get(key, hashKey, interval);
        } else {
            LocalDateTime lastCandleTimeBeforeDate = elasticsearchProcessingService.getLastCandleTimeBeforeDate(candleTime, key);
            if (isNull(lastCandleTimeBeforeDate)) {
                return CandleModel.empty(BigDecimal.ZERO, defaultPreviousCandleTime);
            }

            final String id = ElasticsearchGeneratorUtil.generateId(lastCandleTimeBeforeDate);

            previousModel = elasticsearchProcessingService.get(key, id);
        }
        return previousModel;
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