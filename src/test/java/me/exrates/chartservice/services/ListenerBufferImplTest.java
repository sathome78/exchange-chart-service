package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.impl.ListenerBufferImpl;
import org.apache.commons.lang3.ObjectUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Bean;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

public class ListenerBufferImplTest extends AbstractUnitTest {

    private static final int POOL_SIZE = 20;
    private static final int GENERATED_TRADES_COUNT = 5;

    @Autowired
    private ListenerBuffer listenerBuffer;

    @Autowired
    private TradeDataService tradeDataService;

    @Autowired
    private RedisProcessingService redisProcessingService;


    @Captor
    private ArgumentCaptor<ArrayList<TradeDataDto>> usdCaptor;
    @Captor
    private ArgumentCaptor<ArrayList<TradeDataDto>> usdtCaptor;

    @Test
    public void receive() throws InterruptedException {

        ExecutorService executor = Executors.newFixedThreadPool(POOL_SIZE);

        List<TradeDataDto> usdTrades = generateTestListForPair(BTC_USD);
        List<TradeDataDto> usdtTrades = generateTestListForPair(BTC_USDT);

        doReturn(null).when(redisProcessingService).getLastInitializedCandleTimeFromCache(any());

        Collection<Callable<ObjectUtils.Null>> tasks = new ArrayList<>();
        Stream.concat(usdTrades.stream(),
                      usdtTrades.stream())
                .collect(Collectors.toList())
                .forEach(p -> tasks.add(() -> {
            listenerBuffer.receive(p);
            return null;
        }));

        executor.invokeAll(tasks);

        verify(tradeDataService, times(1))
                .handleReceivedTrades(eq(BTC_USD), usdCaptor.capture());
        verify(tradeDataService, times(1))
                .handleReceivedTrades(eq(BTC_USDT), usdtCaptor.capture());

        assertThat(usdTrades, containsInAnyOrder(usdCaptor.getValue().toArray()));
        assertThat(usdtTrades, containsInAnyOrder(usdtCaptor.getValue().toArray()));
        Assert.assertEquals(GENERATED_TRADES_COUNT, usdCaptor.getValue().size());
        Assert.assertEquals(GENERATED_TRADES_COUNT, usdtCaptor.getValue().size());
    }

    @Test
    public void isReadyToClose() throws InterruptedException {

        receive();

        boolean result = listenerBuffer.isReadyToClose();

        Assert.assertTrue(result);
    }

    private List<TradeDataDto> generateTestListForPair(String currencyPair) {
        List<TradeDataDto> trades = new ArrayList<>();
        int counter = 0;
        while (counter < GENERATED_TRADES_COUNT) {
            counter++;
            trades.add(TradeDataDto.createTradeWithRandomTime(currencyPair));
        }
        return trades;
    }

    @TestConfiguration
    static class TradeDataServiceImplTestContextConfiguration {

        @MockBean
        private TradeDataService tradeDataService;

        @MockBean
        public RedisProcessingService redisProcessingService;

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
        public ListenerBuffer listenerBuffer() {
            return new ListenerBufferImpl(tradeDataService, redisProcessingService);
        }
    }
}
