package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CandlesDataDto;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static me.exrates.chartservice.utils.TimeUtil.getNearestBackTimeForBackdealInterval;


@Log4j2
@Service
public class TradeDataServiceImpl implements TradeDataService {

    private static final long CANDLES_TO_STORE_IN_CACHE = 300;
    private static final BackDealInterval SMALLEST_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final RedisProcessingService redisProcessingService;
    private final XSync<String> xSync;

    public TradeDataServiceImpl(ElasticsearchProcessingService elasticsearchProcessingService,
                                RedisProcessingService redisProcessingService,
                                XSync<String> xSync) {
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.redisProcessingService = redisProcessingService;
        this.xSync = xSync;
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
        List<CandleModel> candleModels;

        LocalDateTime fromTime = getNearestBackTimeForBackdealInterval(from, interval);
        LocalDateTime toTime = getNearestBackTimeForBackdealInterval(to, interval);
        LocalDateTime oldestCachedCandleTime = getCandleTimeByCount(CANDLES_TO_STORE_IN_CACHE, interval);

        final String key = RedisGeneratorUtil.generateKey(pairName);

        if (fromTime.isBefore(oldestCachedCandleTime)) {
            candleModels = Stream.of(redisProcessingService.getByRange(oldestCachedCandleTime, toTime, key, interval),
                    getCandlesFromElasticAndAggregateToInterval(pairName, fromTime, oldestCachedCandleTime.minusSeconds(1), interval))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        } else {
            candleModels = redisProcessingService.getByRange(fromTime, toTime, key, interval);
        }
        return new CandlesDataDto(candleModels, pairName, interval);
    }

    private LocalDateTime getCandleTimeByCount(long count, BackDealInterval interval) {
        LocalDateTime timeForLastCandle = getNearestBackTimeForBackdealInterval(LocalDateTime.now(), interval);
        long candlesFromBeginInterval = interval.getIntervalValue() * count;
        return timeForLastCandle.minus(candlesFromBeginInterval, interval.getIntervalType().getCorrespondingTimeUnit());
    }

    @Override
    public void handleReceivedTrade(TradeDataDto dto) {
        /*todo: update method for new business-logic*/
        xSync.execute(dto.getPairName(), () -> {
            /*iterate all intervals and update existing candles, or save new*/
        });
        checkIsNeededUpdateStorageCandles();
    }

    /*save new candles to elasticsearch*/
    private void checkIsNeededUpdateStorageCandles() {
        /*todo*/
    }

    private void updateValuesFromNewTrade(TradeDataDto tradeDataDto, CandleModel model) {
        model.setCloseRate(tradeDataDto.getExrate());
        model.setVolume(model.getVolume().add(tradeDataDto.getAmountBase()));
        model.setHighRate(model.getHighRate().max(tradeDataDto.getExrate()));
        model.setLowRate(model.getLowRate().min(tradeDataDto.getExrate()));
    }

    private List<CandleModel> getCandlesFromElasticAndAggregateToInterval(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval) {
        final String index = ElasticsearchGeneratorUtil.generateIndex(pairName);

        return CandleDataConverter.convertByInterval(elasticsearchProcessingService.getByRange(from, to, index), interval);
    }
}