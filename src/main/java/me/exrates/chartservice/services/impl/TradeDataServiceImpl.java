package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CandlesDataDto;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;
import static me.exrates.chartservice.utils.TimeUtils.getNearestBackTimeForBackdealInterval;
import static me.exrates.chartservice.utils.TimeUtils.getNearestTimeBeforeForMinInterval;


@Log4j2
@Service
public class TradeDataServiceImpl implements TradeDataService {

    private static final long CANDLES_TO_STORE_IN_CACHE = 300;
    private static final BackDealInterval SMALLEST_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final RedisProcessingService redisProcessingService;
    private final XSync<String> xSync;
    private final List<BackDealInterval> supportedIntervals;

    @Autowired
    public TradeDataServiceImpl(ElasticsearchProcessingService elasticsearchProcessingService,
                                RedisProcessingService redisProcessingService,
                                XSync<String> xSync) {
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.redisProcessingService = redisProcessingService;
        this.xSync = xSync;
        this.supportedIntervals = getAllSupportedIntervals();
    }

    @Override
    public CandleModel getCandleForCurrentTime(String pairName, BackDealInterval interval) {
        return getCandle(pairName, LocalDateTime.now(), interval);
    }

    private CandleModel getSmallestCandle(String pairName, LocalDateTime dateTime) {
        return getCandle(pairName, dateTime, SMALLEST_INTERVAL);
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
        LocalDateTime oldestCachedCandleTime = getCandleTimeByCount(CANDLES_TO_STORE_IN_CACHE, interval);

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
           .forEach((k,v) -> handleReceivedTrades(pairName, k, v));
    }

    private void handleReceivedTrades(String pairName, LocalDateTime candleTime, List<TradeDataDto> dto) {
        xSync.execute(pairName, () -> {
            CandleModel reducedCandle = CandleModel.reduceToCandle(dto);
            supportedIntervals.forEach(p -> {
                CandleModel cachedCandleModel = redisProcessingService.get(pairName, candleTime, p);
                CandleModel mergedCandle = CandleModel.merge(cachedCandleModel, reducedCandle);
                redisProcessingService.insertOrUpdate(mergedCandle, pairName, p);
                if (p.equals(SMALLEST_INTERVAL) && cachedCandleModel == null) {
                    checkAndAddCandlesToElasticCandles(pairName);
                }
            });
        });
    }

    private void checkAndAddCandlesToElasticCandles(String pairName) {
        try {
            LocalDateTime predLastCandleTime = getNearestTimeBeforeForMinInterval(LocalDateTime.now());
            if (elasticsearchProcessingService.exist(pairName, predLastCandleTime)) {
                return;
            }
            LocalDateTime oldestCachedCandleTime = getCandleTimeByCount(CANDLES_TO_STORE_IN_CACHE, SMALLEST_INTERVAL);
            LocalDateTime lastStoredCandleTime = getLastCandleTimeExistsInStorage(pairName, oldestCachedCandleTime, predLastCandleTime)
                    .orElse(oldestCachedCandleTime);
            List<CandleModel> absenteeCandles = redisProcessingService.getByRange(lastStoredCandleTime, predLastCandleTime, pairName, SMALLEST_INTERVAL);
            if (CollectionUtils.isEmpty(absenteeCandles)) {
                elasticsearchProcessingService.batchInsert(absenteeCandles, pairName);
            }
        } catch (Exception e) {
            log.error("error update nonexistent candles in elastic or pair {}", pairName);
        }
    }

    private Optional<LocalDateTime> getLastCandleTimeExistsInStorage(String currencyPair, LocalDateTime fromTime, LocalDateTime toTime) {
        return elasticsearchProcessingService.getByRange(fromTime, toTime, currencyPair)
                .stream()
                .map(CandleModel::getCandleOpenTime )
                .max(LocalDateTime::compareTo);
    }

    private void updateValuesFromNewTrade(TradeDataDto tradeDataDto, CandleModel model) {
        if (model.getLastTradeTime().isBefore(tradeDataDto.getTradeDate())) {
            model.setCloseRate(tradeDataDto.getExrate());
        }
        model.setVolume(model.getVolume().add(tradeDataDto.getAmountBase()));
        model.setHighRate(model.getHighRate().max(tradeDataDto.getExrate()));
        model.setLowRate(model.getLowRate().min(tradeDataDto.getExrate()));
    }

    private CandleModel getCandle(String pairName, LocalDateTime dateTime, BackDealInterval interval) {
        LocalDateTime candleTime = getNearestBackTimeForBackdealInterval(dateTime, interval);
        return redisProcessingService.get(pairName, candleTime, interval);
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

    private List<BackDealInterval> getAllSupportedIntervals() {
        return Stream.of(IntervalType.values())
                     .map(p -> (IntStream.of(p.getSupportedValues())
                                         .mapToObj(v -> new BackDealInterval(v, p))
                                         .collect(Collectors.toList())))
                     .flatMap(Collection::stream)
                     .collect(Collectors.toList());
    }

}
