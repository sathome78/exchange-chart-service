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
    private static final String BTC_USD = "BTC/USD";

    @Mock
    private ElasticsearchProcessingService elasticsearchProcessingService;
    @Mock
    private OrderService orderService;

    private DataInitializerService dataInitializerService;

    @Before
    public void setUp() throws Exception {
        dataInitializerService = spy(new DataInitializerServiceImpl(
                elasticsearchProcessingService,
                orderService));
    }

    @Test
    public void generate_ok() {
        doReturn(Collections.singletonList(OrderDto.builder()
                .id(1)
                .currencyPairName(BTC_USD)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .build()))
                .when(orderService)
                .getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .batchInsert(anyList(), anyString());

        dataInitializerService.generate(FROM_DATE, TO_DATE, BTC_USD, false);

        verify(orderService, atLeastOnce()).getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).batchInsert(anyList(), anyString());
        verify(elasticsearchProcessingService, never()).deleteDataByIndex(anyString());
        verify(elasticsearchProcessingService, never()).deleteIndex(anyString());
    }

    @Test
    public void generate_ok_with_regenerate_data() {
        doReturn(Collections.singletonList(OrderDto.builder()
                .id(1)
                .currencyPairName(BTC_USD)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .build()))
                .when(orderService)
                .getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .batchInsert(anyList(), anyString());
        doReturn(1L)
                .when(elasticsearchProcessingService)
                .deleteDataByIndex(anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .deleteIndex(anyString());

        dataInitializerService.generate(FROM_DATE, TO_DATE, BTC_USD, true);

        verify(orderService, atLeastOnce()).getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).batchInsert(anyList(), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).deleteDataByIndex(anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).deleteIndex(anyString());
    }

    @Test
    public void generate_empty_orders_list() {
        doReturn(Collections.emptyList())
                .when(orderService)
                .getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());

        dataInitializerService.generate(FROM_DATE, TO_DATE, BTC_USD, false);

        verify(orderService, atLeastOnce()).getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, never()).batchInsert(anyList(), anyString());
        verify(elasticsearchProcessingService, never()).deleteDataByIndex(anyString());
        verify(elasticsearchProcessingService, never()).deleteIndex(anyString());
    }
}