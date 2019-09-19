package me.exrates.chartservice.services;

import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.services.impl.DataInitializerServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class DataInitializerServiceTest extends AbstractTest {

    private static final LocalDate NOW = LocalDate.now();
    private static final LocalDate FROM_DATE = NOW.minusDays(1);
    private static final LocalDate TO_DATE = NOW.plusDays(1);

    @Mock
    private ElasticsearchProcessingService elasticsearchProcessingService;
    @Mock
    private OrderService orderService;
    @Mock
    private CacheDataInitializerService cacheDataInitializerService;

    private DataInitializerService dataInitializerService;

    @Before
    public void setUp() throws Exception {
        dataInitializerService = spy(new DataInitializerServiceImpl(
                3,
                elasticsearchProcessingService,
                orderService, cacheDataInitializerService));
    }

    @Test
    public void generate_ok1() {
        doReturn(Collections.singletonList(TEST_PAIR))
                .when(orderService)
                .getAllCurrencyPairNames();
        doReturn(Collections.singletonList(OrderDto.builder()
                .id(1)
                .currencyPairName(TEST_PAIR)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .build()))
                .when(orderService)
                .getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .bulkInsertOrUpdate(anyMap(), anyString());
        doNothing()
                .when(cacheDataInitializerService)
                .updateCacheByIndexAndId(anyString(), anyString());

        dataInitializerService.generate(FROM_DATE, TO_DATE);

        verify(orderService, atLeastOnce()).getAllCurrencyPairNames();
        verify(orderService, atLeastOnce()).getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).bulkInsertOrUpdate(anyMap(), anyString());
        verify(cacheDataInitializerService, atLeastOnce()).updateCacheByIndexAndId(anyString(), anyString());
    }

    @Test
    public void generate_ok2() {
        doReturn(Collections.singletonList(OrderDto.builder()
                .id(1)
                .currencyPairName(TEST_PAIR)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .build()))
                .when(orderService)
                .getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .bulkInsertOrUpdate(anyMap(), anyString());
        doNothing()
                .when(cacheDataInitializerService)
                .updateCacheByIndexAndId(anyString(), anyString());

        dataInitializerService.generate(FROM_DATE, TO_DATE, Collections.singletonList(TEST_PAIR));

        verify(orderService, atLeastOnce()).getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).bulkInsertOrUpdate(anyMap(), anyString());
        verify(cacheDataInitializerService, atLeastOnce()).updateCacheByIndexAndId(anyString(), anyString());
    }

    @Test
    public void generate_empty_orders_list() {
        doReturn(Collections.emptyList())
                .when(orderService)
                .getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());

        dataInitializerService.generate(FROM_DATE, TO_DATE, Collections.singletonList(TEST_PAIR));

        verify(orderService, atLeastOnce()).getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, never()).insert(anyList(), anyString(), anyString());
    }
}