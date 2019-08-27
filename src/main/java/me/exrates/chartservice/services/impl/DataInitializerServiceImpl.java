package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.services.CacheDataInitializerService;
import me.exrates.chartservice.services.DataInitializerService;
import me.exrates.chartservice.services.ElasticsearchProcessingService;
import me.exrates.chartservice.services.OrderService;
import me.exrates.chartservice.utils.ElasticsearchGeneratorUtil;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.TimeUnit;

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
        StopWatch stopWatch = StopWatch.createStarted();
        log.info("<<< GENERATOR >>> Start generate data for cache for: {}", pairName);

        log.info("<<< GENERATOR >>> Start get closed orders from database");
        final List<OrderDto> orders = orderService.getFilteredOrders(fromDate, toDate, pairName);
        log.info("<<< GENERATOR >>> End get closed orders from database, number of orders is: {}", orders.size());

        if (CollectionUtils.isEmpty(orders)) {
            return;
        }

        log.info("<<< GENERATOR >>> Start transform orders to candles");
        List<CandleModel> models = CandleDataConverter.convert(orders);
        log.info("<<< GENERATOR >>> End transform orders to candles, number of 5 minute candles is: {}", models.size());

        log.info("<<< GENERATOR >>> Start fix candles open rate");
        CandleDataConverter.fixOpenRate(models);
        log.info("<<< GENERATOR >>> End fix candles open rate");

        final String index = ElasticsearchGeneratorUtil.generateIndex(pairName);

        log.info("<<< GENERATOR >>> Start save candles in elasticsearch cluster");
        elasticsearchProcessingService.batchInsertOrUpdate(models, index);
        log.info("<<< GENERATOR >>> End save candles in elasticsearch cluster");

        log.info("<<< GENERATOR >>> End generate data for cache for: {}, Time: {} ms", pairName, stopWatch.getTime(TimeUnit.SECONDS));
    }
}