package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CoinmarketcapApiDto;
import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.DailyDataModel;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.CoinmarketcapService;
import me.exrates.chartservice.services.OrderService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static me.exrates.chartservice.configuration.CommonConfiguration.DEFAULT_INTERVAL;

@Log4j2
@Service
public class CoinmarketcapServiceImpl implements CoinmarketcapService {

    private final RedisProcessingService redisProcessingService;
    private final OrderService orderService;

    @Autowired
    public CoinmarketcapServiceImpl(RedisProcessingService redisProcessingService,
                                    OrderService orderService) {
        this.redisProcessingService = redisProcessingService;
        this.orderService = orderService;
    }

    @Override
    public void cleanDailyData() {
        redisProcessingService.getDailyDataKeys().forEach(key -> redisProcessingService.getDailyDataByKey(key).stream()
                .map(DailyDataModel::getCandleOpenTime)
                .filter(candleOpenTime -> candleOpenTime.isBefore(TimeUtil.getOneDayBeforeNowTime()))
                .forEach(candleOpenTime -> {
                    final String hashKey = RedisGeneratorUtil.generateHashKeyForCoinmarketcapData(candleOpenTime);

                    redisProcessingService.deleteDailyData(key, hashKey);
                }));
    }

    @Override
    public void generate() {
        int minutes = TimeUtil.convertToMinutes(new BackDealInterval(1, IntervalType.DAY));

        LocalDateTime now = LocalDateTime.now();
        final LocalDateTime from = now.minusMinutes(minutes);
        final LocalDateTime to = now;

        orderService.getCurrencyPairsFromCache(null).forEach(currencyPairDto -> {
            final String pairName = currencyPairDto.getName();

            StopWatch stopWatch = StopWatch.createStarted();
            log.info("<<< GENERATOR >>> Start generate daily data for pair: {}", pairName);

            try {
                orderService.getAllOrders(from, to, pairName).stream()
                        .collect(Collectors.groupingBy(dto -> TimeUtil.getNearestTimeBeforeForMinInterval(dto.getDateCreation())))
                        .forEach((candleDateTime, orders) -> {
                            BigDecimal highestBid = orders.stream()
                                    .filter(dto -> dto.getOperationTypeId() == 4)
                                    .map(OrderDto::getExRate)
                                    .filter(Objects::nonNull)
                                    .max(Comparator.naturalOrder())
                                    .orElse(null);

                            BigDecimal lowestAsk = orders.stream()
                                    .filter(dto -> dto.getOperationTypeId() == 3)
                                    .map(OrderDto::getExRate)
                                    .filter(Objects::nonNull)
                                    .min(Comparator.naturalOrder())
                                    .orElse(null);

                            DailyDataModel dataModel = new DailyDataModel(candleDateTime, highestBid, lowestAsk);

                            final String key = RedisGeneratorUtil.generateKeyForCoinmarketcapData(pairName);
                            final String hashKey = RedisGeneratorUtil.generateHashKeyForCoinmarketcapData(candleDateTime);

                            redisProcessingService.insertDailyData(dataModel, key, hashKey);
                        });
            } catch (Exception ex) {
                log.error("<<< GENERATOR >>> Process of generation daily data was failed for pair: {}", pairName, ex);
            }
            log.info("<<< GENERATOR >>> End generate daily data for pair: {}. Time: {} s", pairName, stopWatch.getTime(TimeUnit.SECONDS));
        });
    }

    @Override
    public List<CoinmarketcapApiDto> getData(String pairName, BackDealInterval interval) {
        int minutes = TimeUtil.convertToMinutes(interval);

        LocalDateTime now = LocalDateTime.now();
        final LocalDateTime from = TimeUtil.getNearestTimeBeforeForMinInterval(now.minusMinutes(minutes));
        final LocalDateTime to = TimeUtil.getNearestTimeBeforeForMinInterval(now);

        if (Objects.nonNull(pairName)) {
            CurrencyPairDto currencyPairDto = orderService.getCurrencyPairsFromCache(pairName).get(0);

            List<CandleModel> models = getFilteredModels(pairName, from, to);

            CoinmarketcapApiDto coinmarketcapApiDto = CandleDataConverter.reduceToCoinmarketcapData(models, currencyPairDto);

            if (Objects.nonNull(coinmarketcapApiDto)) {
                updateCoinmarketcapDailyData(coinmarketcapApiDto);
            }

            return Collections.singletonList(coinmarketcapApiDto);
        }
        return orderService.getCurrencyPairsFromCache(null).stream()
                .map(currencyPairDto -> {
                    List<CandleModel> models = getFilteredModels(currencyPairDto.getName(), from, to);

                    CoinmarketcapApiDto coinmarketcapApiDto = CandleDataConverter.reduceToCoinmarketcapData(models, currencyPairDto);

                    if (Objects.nonNull(coinmarketcapApiDto)) {
                        updateCoinmarketcapDailyData(coinmarketcapApiDto);
                    }

                    return coinmarketcapApiDto;
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private List<CandleModel> getFilteredModels(String pairName, LocalDateTime from, LocalDateTime to) {
        List<CandleModel> models = getCandlesFromRedis(pairName, from.toLocalDate(), to.toLocalDate());

        return models.stream()
                .filter(model -> (model.getCandleOpenTime().isAfter(from) || model.getCandleOpenTime().isEqual(from)) &&
                        (model.getCandleOpenTime().isBefore(to) || model.getCandleOpenTime().isEqual(to)))
                .collect(Collectors.toList());
    }

    private List<CandleModel> getCandlesFromRedis(String pairName, LocalDate fromDate, LocalDate toDate) {
        final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

        List<CandleModel> bufferedModels = new ArrayList<>();
        while (fromDate.isBefore(toDate) || fromDate.isEqual(toDate)) {
            final String key = RedisGeneratorUtil.generateKey(fromDate);

            try {
                List<CandleModel> models = redisProcessingService.get(key, hashKey, DEFAULT_INTERVAL);
                if (!CollectionUtils.isEmpty(models)) {
                    bufferedModels.addAll(models);
                }
            } catch (Exception ex) {
                log.error(ex);
            }

            fromDate = fromDate.plusDays(1);
        }
        return bufferedModels;
    }

    private void updateCoinmarketcapDailyData(CoinmarketcapApiDto coinmarketcapApiDto) {
        final String key = RedisGeneratorUtil.generateKeyForCoinmarketcapData(coinmarketcapApiDto.getCurrencyPairName());

        List<DailyDataModel> dailyDataList = redisProcessingService.getDailyDataByKey(key);

        BigDecimal highestBid = dailyDataList.stream()
                .map(DailyDataModel::getHighestBid)
                .filter(Objects::nonNull)
                .max(Comparator.naturalOrder())
                .orElse(null);

        coinmarketcapApiDto.setHighestBid(highestBid);

        BigDecimal lowestAsk = dailyDataList.stream()
                .map(DailyDataModel::getLowestAsk)
                .filter(Objects::nonNull)
                .min(Comparator.naturalOrder())
                .orElse(null);

        coinmarketcapApiDto.setLowestAsk(lowestAsk);
    }
}