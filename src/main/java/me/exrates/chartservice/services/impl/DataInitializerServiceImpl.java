package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.services.DataInitializerService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.OrderService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Log4j2
@Service
public class DataInitializerServiceImpl implements DataInitializerService {

    private final long minPeriod;

    private final ElasticsearchProcessingService elasticsearchProcessingService;
    private final OrderService orderService;

    @Autowired
    public DataInitializerServiceImpl(@Value("${generator.min-period:3}") long minPeriod,
                                      ElasticsearchProcessingService elasticsearchProcessingService,
                                      OrderService orderService) {
        this.minPeriod = minPeriod;
        this.elasticsearchProcessingService = elasticsearchProcessingService;
        this.orderService = orderService;
    }

    @Override
    public void generate(LocalDate fromDate, LocalDate toDate) {
        final List<String> pairs = orderService.getAllCurrencyPairNames().stream()
                .distinct()
                .sorted(Comparator.naturalOrder())
                .collect(Collectors.toList());

        generate(fromDate, toDate, pairs);
    }

    @Override
    public void generate(LocalDate fromDate, LocalDate toDate, List<String> pairs) {
        pairs.forEach(pair -> generateForAllPeriod(fromDate, toDate, pair));
    }

    private void generateForAllPeriod(LocalDate fromDate, LocalDate toDate, String pair) {
        LocalDate minFrom = fromDate;
        LocalDate minTo = fromDate.plusMonths(minPeriod);

        while (minTo.isBefore(toDate)) {
            generateForMinPeriod(minFrom, minTo, pair);

            minFrom = minTo;
            minTo = minTo.plusMonths(minPeriod);
        }
        generateForMinPeriod(minFrom, toDate, pair);
    }

    private void generateForMinPeriod(LocalDate fromDate, LocalDate toDate, String pair) {
        try {
            StopWatch stopWatch = StopWatch.createStarted();
            log.info("<<< GENERATOR >>> Start generate cache data for pair: {} [Period: {} - {}]", pair, fromDate, toDate);

            log.debug("<<< GENERATOR >>> Start get closed orders from database");
            final List<OrderDto> orders = orderService.getFilteredOrders(fromDate, toDate, pair);
            log.debug("<<< GENERATOR >>> End get closed orders from database, number of orders is: {}", orders.size());

            if (CollectionUtils.isEmpty(orders)) {
                return;
            }

            log.debug("<<< GENERATOR >>> Start transform orders to candles");
            List<CandleModel> models = CandleDataConverter.convert(orders);
            log.debug("<<< GENERATOR >>> End transform orders to candles, number of 5 minute candles is: {}", models.size());

            log.debug("<<< GENERATOR >>> Start fix candles open rate");
            CandleDataConverter.fixOpenRate(models);
            log.debug("<<< GENERATOR >>> End fix candles open rate");

            final String index = ElasticsearchGeneratorUtil.generateIndex(pair);

            log.debug("<<< GENERATOR >>> Start save candles in elasticsearch cluster");
            elasticsearchProcessingService.bulkInsertOrUpdate(models, index);
            log.debug("<<< GENERATOR >>> End save candles in elasticsearch cluster");

            log.info("<<< GENERATOR >>> End generate cache data for pair: {} [Period: {} - {}]. Time: {} s", pair, fromDate, toDate, stopWatch.getTime(TimeUnit.SECONDS));
        } catch (Exception ex) {
            log.error("<<< GENERATOR >>> Process of generation cache data was failed for pair: {} [Period: {} - {}]", pair, fromDate, toDate, ex);
        }
    }
}