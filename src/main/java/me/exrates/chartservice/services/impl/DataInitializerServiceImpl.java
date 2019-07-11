package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.DataInitializerService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.OrderService;
import me.exrates.chartservice.utils.TimeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Log4j2
@Service
public class DataInitializerServiceImpl implements DataInitializerService {

    private final ElasticsearchProcessingService processingService;
    private final OrderService orderService;

    @Autowired
    public DataInitializerServiceImpl(ElasticsearchProcessingService processingService,
                                      OrderService orderService) {
        this.processingService = processingService;
        this.orderService = orderService;
    }

    @Override
    public void generate(LocalDate fromDate, LocalDate toDate, String pairName, boolean regenerate) {
        final List<OrderDto> orders = orderService.getFilteredOrders(fromDate, toDate, pairName);

        Map<LocalDateTime, List<TradeDataDto>> groupedByTradeDate = orders.stream()
                .map(TradeDataDto::new)
                .collect(Collectors.groupingBy(tradeData -> TimeUtils.getNearestTimeBefore(tradeData.getTradeDate())));

        List<CandleModel> candleModels = groupedByTradeDate.entrySet().stream()
                .map(entry -> {
                    final LocalDateTime candleOpenTime = entry.getKey();
                    final List<TradeDataDto> trades = entry.getValue();

                    TradeDataDto min = Collections.min(trades, Comparator.comparing(TradeDataDto::getTradeDate));
                    TradeDataDto max = Collections.max(trades, Comparator.comparing(TradeDataDto::getTradeDate));

                    BigDecimal highRate = trades.stream().map(TradeDataDto::getExrate).max(Comparator.naturalOrder()).orElse(BigDecimal.ZERO);
                    BigDecimal lowRate = trades.stream().map(TradeDataDto::getExrate).min(Comparator.naturalOrder()).orElse(BigDecimal.ZERO);

                    return CandleModel.builder()
                            .openRate(min.getExrate())
                            .closeRate(max.getExrate())
                            .highRate(highRate)
                            .lowRate(lowRate)
                            .lastTradeTime(max.getTradeDate())
                            .candleOpenTime(candleOpenTime)
                            .build();
                })
                .collect(Collectors.toList());

        if (regenerate) {
            processingService.deleteByIndex(pairName);
        }

        processingService.batchInsert(candleModels, pairName);
    }
}