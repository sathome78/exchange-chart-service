package me.exrates.chartservice.services;

import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.DailyDataModel;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.services.impl.DataInitializerServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
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
    private RedisProcessingService redisProcessingService;
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
                redisProcessingService,
                orderService,
                cacheDataInitializerService));
    }

    @Test
    public void generateCandleData_ok1() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .name(TEST_PAIR)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);
        doReturn(Collections.singletonList(OrderDto.builder()
                .currencyPairName(TEST_PAIR)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .build()))
                .when(orderService)
                .getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .bulkInsertOrUpdate(anyMap(), anyString());
        doNothing()
                .when(cacheDataInitializerService)
                .updateCacheByIndexAndId(anyString(), anyString());

        dataInitializerService.generateCandleData(FROM_DATE, TO_DATE);

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(orderService, atLeastOnce()).getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).bulkInsertOrUpdate(anyMap(), anyString());
        verify(cacheDataInitializerService, atLeastOnce()).updateCacheByIndexAndId(anyString(), anyString());
    }

    @Test
    public void generateCandleData_empty_currency_pairs_list1() {
        doReturn(Collections.emptyList())
                .when(orderService)
                .getCurrencyPairsFromCache(null);

        dataInitializerService.generateCandleData(FROM_DATE, TO_DATE);

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(orderService, never()).getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, never()).bulkInsertOrUpdate(anyMap(), anyString());
        verify(cacheDataInitializerService, never()).updateCacheByIndexAndId(anyString(), anyString());
    }

    @Test
    public void generateCandleData_ok2() {
        doReturn(Collections.singletonList(OrderDto.builder()
                .currencyPairName(TEST_PAIR)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .build()))
                .when(orderService)
                .getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        doNothing()
                .when(elasticsearchProcessingService)
                .bulkInsertOrUpdate(anyMap(), anyString());
        doNothing()
                .when(cacheDataInitializerService)
                .updateCacheByIndexAndId(anyString(), anyString());

        dataInitializerService.generateCandleData(FROM_DATE, TO_DATE, Collections.singletonList(TEST_PAIR));

        verify(orderService, atLeastOnce()).getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, atLeastOnce()).bulkInsertOrUpdate(anyMap(), anyString());
        verify(cacheDataInitializerService, atLeastOnce()).updateCacheByIndexAndId(anyString(), anyString());
    }

    @Test
    public void generateCandleData_empty_currency_pairs_list2() {
        dataInitializerService.generateCandleData(FROM_DATE, TO_DATE, Collections.emptyList());

        verify(orderService, never()).getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, never()).bulkInsertOrUpdate(anyMap(), anyString());
        verify(cacheDataInitializerService, never()).updateCacheByIndexAndId(anyString(), anyString());
    }

    @Test
    public void generateCandleData_empty_orders_list() {
        doReturn(Collections.emptyList())
                .when(orderService)
                .getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());

        dataInitializerService.generateCandleData(FROM_DATE, TO_DATE, Collections.singletonList(TEST_PAIR));

        verify(orderService, atLeastOnce()).getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());
        verify(elasticsearchProcessingService, never()).insert(anyList(), anyString(), anyString());
    }

    @Test
    public void generateDailyData_ok() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .hidden(false)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);
        doReturn(Collections.singletonList(OrderDto.builder()
                .currencyPairName(TEST_PAIR)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .dateCreation(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .operationTypeId(4)
                .build()))
                .when(orderService)
                .getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        doNothing()
                .when(redisProcessingService)
                .insertDailyData(any(DailyDataModel.class), anyString(), anyString());

        dataInitializerService.generateDailyData();

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(orderService, atLeastOnce()).getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        verify(redisProcessingService, atLeastOnce()).insertDailyData(any(DailyDataModel.class), anyString(), anyString());
    }

    @Test
    public void generateDailyData_empty_currencies_list() {
        doReturn(Collections.emptyList())
                .when(orderService)
                .getCurrencyPairsFromCache(null);

        dataInitializerService.generateDailyData();

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(orderService, never()).getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        verify(redisProcessingService, never()).insertDailyData(any(DailyDataModel.class), anyString(), anyString());
    }

    @Test
    public void generateDailyData_empty_orders_list() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .id(1)
                .name(TEST_PAIR)
                .hidden(false)
                .build()))
                .when(orderService)
                .getCurrencyPairsFromCache(null);
        doReturn(Collections.emptyList())
                .when(orderService)
                .getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());

        dataInitializerService.generateDailyData();

        verify(orderService, atLeastOnce()).getCurrencyPairsFromCache(null);
        verify(orderService, atLeastOnce()).getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
        verify(redisProcessingService, never()).insertDailyData(any(DailyDataModel.class), anyString(), anyString());
    }
}