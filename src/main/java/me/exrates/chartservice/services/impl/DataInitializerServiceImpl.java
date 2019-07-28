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
import java.util.concurrent.CompletableFuture;
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
        log.debug("<<< GENERATOR >>> Start generate data for cache for: {}", pairName);

        log.debug("<<< GENERATOR >>> Start get closed orders from database");
        final List<OrderDto> orders = orderService.getFilteredOrders(fromDate, toDate, pairName);
        log.debug("<<< GENERATOR >>> End get closed orders from database");

        if (CollectionUtils.isEmpty(orders)) {
            return;
        }

        log.debug("<<< GENERATOR >>> Start transform orders to candles");
        List<CandleModel> models = CandleDataConverter.convert(orders);
        log.debug("<<< GENERATOR >>> End transform orders to candles");

        final String index = ElasticsearchGeneratorUtil.generateIndex(pairName);

        log.debug("<<< GENERATOR >>> Start save candles in elasticsearch cluster");
        elasticsearchProcessingService.batchInsertOrUpdate(models, index);
        log.debug("<<< GENERATOR >>> End save candles in elasticsearch cluster");

        log.debug("<<< GENERATOR >>> Start async process of generate all intervals cache for: {}", pairName);
        CompletableFuture.runAsync(() -> cacheDataInitializerService.updateCacheByKey(index));

        log.debug("<<< GENERATOR >>> End generate data for cache for: {}, Time: {} ms", pairName, stopWatch.getTime(TimeUnit.SECONDS));
    }
}