package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CoinmarketcapApiDto;
import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.DailyDataModel;
import me.exrates.chartservice.model.ExchangeRatesDto;
import me.exrates.chartservice.services.DailyDataService;
import me.exrates.chartservice.services.OrderService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
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
import java.util.stream.Collectors;

import static me.exrates.chartservice.configuration.CommonConfiguration.DEFAULT_INTERVAL;

@Log4j2
@Service
public class DailyDataServiceImpl implements DailyDataService {

    private final RedisProcessingService redisProcessingService;
    private final OrderService orderService;

    @Autowired
    public DailyDataServiceImpl(RedisProcessingService redisProcessingService,
                                OrderService orderService) {
        this.redisProcessingService = redisProcessingService;
        this.orderService = orderService;
    }

    @Override
    public List<CoinmarketcapApiDto> getCoinmarketcapData(String pairName, BackDealInterval interval) {
        int minutes = TimeUtil.convertToMinutes(interval);

        LocalDateTime now = LocalDateTime.now();
        final LocalDateTime from = TimeUtil.getNearestTimeBeforeForMinInterval(now.minusMinutes(minutes));
        final LocalDateTime to = TimeUtil.getNearestTimeBeforeForMinInterval(now);

        if (Objects.nonNull(pairName)) {
            CurrencyPairDto currencyPairDto = orderService.getCurrencyPairsFromCache(pairName).get(0);

            List<CandleModel> models = getFilteredModels(pairName, from, to);

            CoinmarketcapApiDto coinmarketcapApiDto = CandleDataConverter.reduceToCoinmarketcapData(models, currencyPairDto);

            if (Objects.isNull(coinmarketcapApiDto)) {
                return Collections.emptyList();
            }
            updateCoinmarketcapDailyData(coinmarketcapApiDto);

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

    @Override
    public List<ExchangeRatesDto> getExchangeRatesData(String pairName, BackDealInterval interval) {
        int minutes = TimeUtil.convertToMinutes(interval);

        LocalDateTime now = LocalDateTime.now();
        final LocalDateTime from = TimeUtil.getNearestTimeBeforeForMinInterval(now.minusMinutes(minutes));
        final LocalDateTime to = TimeUtil.getNearestTimeBeforeForMinInterval(now);

        if (Objects.nonNull(pairName)) {
            CurrencyPairDto currencyPairDto = orderService.getCurrencyPairsFromCache(pairName).get(0);

            List<CandleModel> models = getFilteredModels(pairName, from, to);

            ExchangeRatesDto exchangeRatesDto = CandleDataConverter.reduceToExchangeRatesData(models, currencyPairDto);

            return Objects.nonNull(exchangeRatesDto) ? Collections.singletonList(exchangeRatesDto) : Collections.emptyList();
        }
        return orderService.getCurrencyPairsFromCache(null).stream()
                .map(currencyPairDto -> {
                    List<CandleModel> models = getFilteredModels(currencyPairDto.getName(), from, to);

                    return CandleDataConverter.reduceToExchangeRatesData(models, currencyPairDto);
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    private List<CandleModel> getFilteredModels(String pairName, LocalDateTime from, LocalDateTime to) {
        List<CandleModel> models = getCandlesFromRedis(pairName, from.toLocalDate(), to.toLocalDate());

        return CandleDataConverter.filterModelsByRange(models, from, to);
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