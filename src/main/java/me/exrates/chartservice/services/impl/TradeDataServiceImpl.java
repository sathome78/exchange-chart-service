package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CandlesDataDto;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static me.exrates.chartservice.utils.TimeUtils.getNearestBackTimeForBackdealInterval;


@Log4j2
@Service
public class TradeDataServiceImpl implements TradeDataService {

    private static final long CANDLES_TO_STORE_IN_CACHE = 300;
    private static final BackDealInterval SMALLEST_INTERVAL = new BackDealInterval("5 MINUTE");

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
        return redisProcessingService.get(pairName, candleTime, interval);
    }

    @Override
    public CandlesDataDto getCandles(String pairName, LocalDateTime from, LocalDateTime to, BackDealInterval interval) {
        List<CandleModel> candleModels;
        LocalDateTime fromTime = getNearestBackTimeForBackdealInterval(from, interval);
        LocalDateTime toTime = getNearestBackTimeForBackdealInterval(to, interval);
        LocalDateTime oldestCachedCandleTime = getCandleTimeByCount(CANDLES_TO_STORE_IN_CACHE, interval);
        if (fromTime.isBefore(oldestCachedCandleTime)) {
            candleModels = Stream.of(redisProcessingService.getByRange(oldestCachedCandleTime, toTime, pairName, interval),
                                     getCandlesFromElasticAndAggregateToInterval(pairName, fromTime, oldestCachedCandleTime.minusSeconds(1), interval))
                    .flatMap(Collection::stream)
                    .collect(Collectors.toList());
        } else {
            candleModels = redisProcessingService.getByRange(fromTime, toTime, pairName, interval);
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
        return transformToInterval(interval, elasticsearchProcessingService.getByRange(from, to, pairName));
    }

    /**@param backDealInterval - interval for aggregating candles
     * @param candleModels - list of candles for aggregating to backDealInterval
     * @return unsorted list of candles, aggregated to specified backDealInterval
     */
    private List<CandleModel> transformToInterval(BackDealInterval backDealInterval, List<CandleModel> candleModels) {
        return candleModels.stream()
                .collect(Collectors.groupingBy(p -> getNearestBackTimeForBackdealInterval(p.getCandleOpenTime(), backDealInterval)))
                .entrySet().stream()
                .map(x -> {
                    List<CandleModel> groupedCandles = x.getValue();
                    groupedCandles.sort(Comparator.comparing(CandleModel::getCandleOpenTime));
                    CandleModel model = groupedCandles.stream()
                            .reduce(null, (left, right) -> new CandleModel(left.getVolume().add(right.getVolume()),
                                                                     left.getLowRate().min(right.getLowRate()),
                                                                     left.getHighRate().max(right.getHighRate())));
                    model.setOpenRate(groupedCandles.get(0).getOpenRate());
                    model.setCloseRate(groupedCandles.get(groupedCandles.size() - 1).getCloseRate());
                    model.setCandleOpenTime(x.getKey());
                    return model;
                })
                .collect(Collectors.toList());
    }

}
