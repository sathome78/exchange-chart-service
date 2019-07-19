package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.CacheDataInitializerService;
import me.exrates.chartservice.services.DataInitializerService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.OrderService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

@Log4j2
@Service
public class DataInitializerServiceImpl implements DataInitializerService {

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final OrderService orderService;
    private final CacheDataInitializerService cacheDataInitializerService;

    @Autowired
    public DataInitializerServiceImpl(ElasticsearchProcessingService elasticsearchProcessingService,
                                      OrderService orderService,
                                      CacheDataInitializerService cacheDataInitializerService) {
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.orderService = orderService;
        this.cacheDataInitializerService = cacheDataInitializerService;
    }

    @Override
    public void generate(LocalDate fromDate, LocalDate toDate, String pairName) {
        final List<OrderDto> orders = orderService.getFilteredOrders(fromDate, toDate, pairName);
        if (CollectionUtils.isEmpty(orders)) {
            return;
        }

        Map<LocalDateTime, List<TradeDataDto>> groupedByTradeDate = orders.stream()
                .map(TradeDataDto::new)
                .collect(Collectors.groupingBy(tradeData -> TimeUtil.getNearestTimeBeforeForMinInterval(tradeData.getTradeDate())));

        List<CandleModel> candleModels = groupedByTradeDate.entrySet().stream()
                .map(entry -> {
                    final List<TradeDataDto> trades = entry.getValue();

                    TradeDataDto min = Collections.min(trades, Comparator.comparing(TradeDataDto::getTradeDate));
                    TradeDataDto max = Collections.max(trades, Comparator.comparing(TradeDataDto::getTradeDate));

                    BigDecimal highRate = trades.stream()
                            .map(TradeDataDto::getExrate)
                            .max(Comparator.naturalOrder())
                            .orElse(BigDecimal.ZERO);
                    BigDecimal lowRate = trades.stream()
                            .map(TradeDataDto::getExrate)
                            .min(Comparator.naturalOrder())
                            .orElse(BigDecimal.ZERO);

                    BigDecimal volume = trades.stream()
                            .map(TradeDataDto::getAmountBase)
                            .reduce(BigDecimal::add)
                            .orElse(BigDecimal.ZERO);

                    return CandleModel.builder()
                            .firstTradeTime(min.getTradeDate())
                            .lastTradeTime(max.getTradeDate())
                            .openRate(min.getExrate())
                            .closeRate(max.getExrate())
                            .highRate(highRate)
                            .lowRate(lowRate)
                            .volume(volume)
                            .lastTradeTime(max.getTradeDate())
                            .candleOpenTime(entry.getKey())
                            .build();
                })
                .sorted(Comparator.comparing(CandleModel::getCandleOpenTime))
                .collect(Collectors.toList());

        final String index = ElasticsearchGeneratorUtil.generateIndex(pairName);

        elasticsearchProcessingService.batchInsertOrUpdate(candleModels, index);

        CompletableFuture.runAsync(cacheDataInitializerService::updateCache);
    }
}