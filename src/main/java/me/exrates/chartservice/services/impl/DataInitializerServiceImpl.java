package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.DailyDataModel;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.CacheDataInitializerService;
import me.exrates.chartservice.services.DataInitializerService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.OrderService;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Log4j2
@Service
public class DataInitializerServiceImpl implements DataInitializerService {

    private final long minPeriod;

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final RedisProcessingService redisProcessingService;
    private final OrderService orderService;
    private final CacheDataInitializerService cacheDataInitializerService;

    @Autowired
    public DataInitializerServiceImpl(@Value("${generator.min-period:3}") long minPeriod,
                                      ElasticsearchProcessingService elasticsearchProcessingService,
                                      RedisProcessingService redisProcessingService,
                                      OrderService orderService,
                                      CacheDataInitializerService cacheDataInitializerService) {
        this.minPeriod = minPeriod;
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.redisProcessingService = redisProcessingService;
        this.orderService = orderService;
        this.cacheDataInitializerService = cacheDataInitializerService;
    }

    @Override
    public void generateCandleData(LocalDate fromDate, LocalDate toDate) {
        final List<String> pairs = orderService.getCurrencyPairsFromCache(null).stream()
                .map(CurrencyPairDto::getName)
                .distinct()
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());

        pairs.forEach(pair -> generateCandleData(fromDate, toDate, pair));
    }

    @Override
    public void generateCandleData(LocalDate fromDate, LocalDate toDate, List<String> pairs) {
        pairs.forEach(pair -> generateCandleData(fromDate, toDate, pair));
    }

    @Override
    public void generateCandleData(LocalDate fromDate, LocalDate toDate, String pair) {
        LocalDate minFrom = fromDate;
        LocalDate minTo = fromDate.plusMonths(minPeriod);

        while (minTo.isBefore(toDate)) {
            generateForMinPeriod(minFrom, minTo, pair);

            minFrom = minTo;
            minTo = minTo.plusMonths(minPeriod);
        }
        generateForMinPeriod(minFrom, toDate, pair);
    }

    private void generateForMinPeriod(LocalDate minFrom, LocalDate minTo, String pair) {
        StopWatch stopWatch = StopWatch.createStarted();
        log.info("<<< GENERATOR >>> Start generate data for pair: {}. Period: {} - {}", pair, minFrom, minTo);

        try {
            final Map<String, List<CandleModel>> mapOfModels = getTransformedData(minFrom, minTo, pair);
            final String id = ElasticsearchGeneratorUtil.generateId(pair);

            log.debug("<<< GENERATOR >>> Start save candles in elasticsearch cluster");
            if (!CollectionUtils.isEmpty(mapOfModels)) {
                elasticsearchProcessingService.bulkInsertOrUpdate(mapOfModels, id);
            }
            log.debug("<<< GENERATOR >>> End save candles in elasticsearch cluster");

            mapOfModels.keySet().forEach(index -> {
                log.debug("<<< GENERATOR >>> Start update cache");
                cacheDataInitializerService.updateCacheByIndexAndId(index, id);
                log.debug("<<< GENERATOR >>> End update cache");
            });
        } catch (Exception ex) {
            log.error("<<< GENERATOR >>> Process of generation data was failed for pair: {} [Period: {} - {}]", pair, minFrom, minTo, ex);
        }
        log.info("<<< GENERATOR >>> End generate data for pair: {}. Period: {} - {}. Time: {} s", pair, minFrom, minTo, stopWatch.getTime(TimeUnit.SECONDS));
    }

    private Map<String, List<CandleModel>> getTransformedData(LocalDate fromDate, LocalDate toDate, String pair) {
        log.debug("<<< GENERATOR >>> Start get closed orders from database");
        final List<OrderDto> closedOrders = orderService.getClosedOrders(fromDate, toDate, pair);
        log.debug("<<< GENERATOR >>> End get closed orders from database, number of orders is: {}", closedOrders.size());

        if (CollectionUtils.isEmpty(closedOrders)) {
            return Collections.emptyMap();
        }

        log.debug("<<< GENERATOR >>> Start divide orders by days");
        Map<LocalDate, List<OrderDto>> mapOfClosedOrders = closedOrders.stream()
                .collect(Collectors.groupingBy(orderDto -> orderDto.getDateAcception().toLocalDate()));
        log.debug("<<< GENERATOR >>> End divide orders by days");

        return mapOfClosedOrders.entrySet().stream()
                .map(entry -> {
                    final LocalDate key = entry.getKey();
                    final List<OrderDto> value = entry.getValue();

                    log.debug("<<< GENERATOR >>> Start transform orders to candles");
                    List<CandleModel> models = CandleDataConverter.convert(value);
                    log.debug("<<< GENERATOR >>> End transform orders to candles, number of 5 minute candles is: {}", models.size());

                    final String index = ElasticsearchGeneratorUtil.generateIndex(key);

                    return Pair.of(index, models);
                })
                .collect(Collectors.toMap(
                        Pair::getKey,
                        Pair::getValue));
    }

    @Override
    public void generateDailyData() {
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
}