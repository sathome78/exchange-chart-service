package me.exrates.chartservice.services;

import com.antkorwin.xsync.XSync;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.impl.ListenerBufferImpl;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.math3.random.RandomDataGenerator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ListenerBufferImplTest extends AbstractTest {

    private static final int POOL_SIZE = 20;
    private static final int GENERATED_TRADES_COUNT = 5;

    @Captor
    private ArgumentCaptor<ArrayList<TradeDataDto>> usdCaptor;
    @Captor
    private ArgumentCaptor<ArrayList<TradeDataDto>> usdtCaptor;

    @Mock
    private TradeDataService tradeDataService;
    @Mock
    private RedisProcessingService redisProcessingService;

    private ListenerBuffer listenerBuffer;

    @Before
    public void setUp() throws Exception {
        listenerBuffer = spy(new ListenerBufferImpl(
                tradeDataService,
                redisProcessingService,
                new XSync<>()));
    }

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
            trades.add(createTradeWithRandomTime(currencyPair));
        }
        return trades;
    }

    public TradeDataDto createTradeWithRandomTime(String currencyPair) {
        TradeDataDto tradeDataDto = new TradeDataDto();
        tradeDataDto.setPairName(currencyPair);
        tradeDataDto.setTradeDate(LocalDateTime.now().plusSeconds(new RandomDataGenerator().nextLong(0, 10000)));
        return tradeDataDto;
    }
}