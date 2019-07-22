package me.exrates.chartservice.services;

import com.antkorwin.xsync.XSync;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.impl.TradeDataServiceImpl;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Configuration
public class UnitTestContext {

    static final int CANDLES_TO_STORE = 300;

    @MockBean
    public ElasticsearchProcessingService elasticsearchProcessingService;

    @MockBean
    public RedisProcessingService redisProcessingService;

    @Bean
    public XSync<String> xSync() {
        return new XSync<>();
    }

    @Bean
    public List<BackDealInterval> supportedIntervals() {
        return Stream.of(IntervalType.values())
                .map(p -> (IntStream.of(p.getSupportedValues())
                        .mapToObj(v -> new BackDealInterval(v, p))
                        .collect(Collectors.toList())))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Bean
    public TradeDataService tradeDataService() {
        return new TradeDataServiceImpl(
                elasticsearchProcessingService,
                redisProcessingService,
                xSync(),
                CANDLES_TO_STORE,
                supportedIntervals()
        );
    }
}
