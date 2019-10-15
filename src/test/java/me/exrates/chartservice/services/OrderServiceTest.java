package me.exrates.chartservice.services;

import me.exrates.chartservice.model.CurrencyPairDto;
import me.exrates.chartservice.model.CurrencyRateDto;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.repositories.OrderRepository;
import me.exrates.chartservice.services.impl.OrderServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.cache.Cache;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class OrderServiceTest extends AbstractTest {

    private static final LocalDate NOW = LocalDate.now();
    private static final LocalDate FROM_DATE = NOW.minusDays(1);
    private static final LocalDate TO_DATE = NOW.plusDays(1);

    @Mock
    Cache currencyPairsCache;

    @Mock
    Cache currencyRatesCache;

    @Mock
    private OrderRepository orderRepository;

    private OrderService orderService;

    @Before
    public void setUp() throws Exception {
        orderService = spy(new OrderServiceImpl(orderRepository, currencyPairsCache, currencyRatesCache));
    }

    @Test
    public void getCurrencyPairsFromCache_ok() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .name(TEST_PAIR)
                .build()))
                .when(currencyPairsCache)
                .get(any(), any(Callable.class));

        List<CurrencyPairDto> list = orderService.getCurrencyPairsFromCache(null);

        assertFalse(CollectionUtils.isEmpty(list));
        assertEquals(1, list.size());
        assertNotNull(list.get(0));
        assertEquals(TEST_PAIR, list.get(0).getName());
        assertFalse(list.get(0).isHidden());

        verify(currencyPairsCache, atLeastOnce()).get(any(), any(Callable.class));
    }

    @Test
    public void getAllCurrencyPairs_ok() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .name(TEST_PAIR)
                .build()))
                .when(orderRepository)
                .getAllCurrencyPairs();

        List<CurrencyPairDto> list = orderService.getAllCurrencyPairs();

        assertFalse(CollectionUtils.isEmpty(list));
        assertEquals(1, list.size());
        assertNotNull(list.get(0));
        assertEquals(TEST_PAIR, list.get(0).getName());
        assertFalse(list.get(0).isHidden());

        verify(orderRepository, atLeastOnce()).getAllCurrencyPairs();
    }

    @Test
    public void getAllCurrencyPairs_empty_list() {
        doReturn(Collections.emptyList())
                .when(orderRepository)
                .getAllCurrencyPairs();

        List<CurrencyPairDto> list = orderService.getAllCurrencyPairs();

        assertTrue(CollectionUtils.isEmpty(list));

        verify(orderRepository, atLeastOnce()).getAllCurrencyPairs();
    }

    @Test
    public void getCurrencyPairByName_ok() {
        doReturn(Collections.singletonList(CurrencyPairDto.builder()
                .name(TEST_PAIR)
                .build()))
                .when(orderRepository)
                .getCurrencyPairByName(anyString());

        List<CurrencyPairDto> list = orderService.getCurrencyPairByName(TEST_PAIR);

        assertFalse(CollectionUtils.isEmpty(list));
        assertEquals(1, list.size());
        assertNotNull(list.get(0));
        assertEquals(TEST_PAIR, list.get(0).getName());
        assertFalse(list.get(0).isHidden());

        verify(orderRepository, atLeastOnce()).getCurrencyPairByName(anyString());
    }

    @Test
    public void getCurrencyPairByName_empty_list() {
        doReturn(Collections.emptyList())
                .when(orderRepository)
                .getCurrencyPairByName(anyString());

        List<CurrencyPairDto> list = orderService.getCurrencyPairByName(TEST_PAIR);

        assertTrue(CollectionUtils.isEmpty(list));

        verify(orderRepository, atLeastOnce()).getCurrencyPairByName(anyString());
    }

    @Test
    public void getAllCurrencyRatesFromCache_ok() {
        doReturn(Collections.singletonList(CurrencyRateDto.builder()
                .currencyName(TEST_PAIR)
                .btcRate(BigDecimal.TEN)
                .usdRate(BigDecimal.ONE)
                .build()))
                .when(currencyRatesCache)
                .get(any(), any(Callable.class));

        List<CurrencyRateDto> list = orderService.getAllCurrencyRatesFromCache(null);

        assertFalse(CollectionUtils.isEmpty(list));
        assertEquals(1, list.size());
        assertNotNull(list.get(0));
        assertEquals(TEST_PAIR, list.get(0).getCurrencyName());
        assertEquals(BigDecimal.TEN, list.get(0).getBtcRate());
        assertEquals(BigDecimal.ONE, list.get(0).getUsdRate());

        verify(currencyRatesCache, atLeastOnce()).get(any(), any(Callable.class));
    }

    @Test
    public void getAllCurrencyRates_ok() {
        doReturn(Collections.singletonList(CurrencyRateDto.builder()
                .currencyName(TEST_PAIR)
                .btcRate(BigDecimal.TEN)
                .usdRate(BigDecimal.ONE)
                .build()))
                .when(orderRepository)
                .getAllCurrencyRates();

        List<CurrencyRateDto> list = orderService.getAllCurrencyRates();

        assertFalse(CollectionUtils.isEmpty(list));
        assertEquals(1, list.size());
        assertNotNull(list.get(0));
        assertEquals(TEST_PAIR, list.get(0).getCurrencyName());
        assertEquals(BigDecimal.TEN, list.get(0).getBtcRate());
        assertEquals(BigDecimal.ONE, list.get(0).getUsdRate());

        verify(orderRepository, atLeastOnce()).getAllCurrencyRates();
    }

    @Test
    public void getAllCurrencyRates_empty_list() {
        doReturn(Collections.emptyList())
                .when(orderRepository)
                .getAllCurrencyRates();

        List<CurrencyRateDto> list = orderService.getAllCurrencyRates();

        assertTrue(CollectionUtils.isEmpty(list));

        verify(orderRepository, atLeastOnce()).getAllCurrencyRates();
    }

    @Test
    public void getCurrencyRateByName_ok() {
        doReturn(Collections.singletonList(CurrencyRateDto.builder()
                .currencyName(TEST_PAIR)
                .btcRate(BigDecimal.TEN)
                .usdRate(BigDecimal.ONE)
                .build()))
                .when(orderRepository)
                .getCurrencyRateByName(anyString());

        List<CurrencyRateDto> list = orderService.getCurrencyRateByName(TEST_PAIR);

        assertFalse(CollectionUtils.isEmpty(list));
        assertEquals(1, list.size());
        assertNotNull(list.get(0));
        assertEquals(TEST_PAIR, list.get(0).getCurrencyName());
        assertEquals(BigDecimal.TEN, list.get(0).getBtcRate());
        assertEquals(BigDecimal.ONE, list.get(0).getUsdRate());

        verify(orderRepository, atLeastOnce()).getCurrencyRateByName(anyString());
    }

    @Test
    public void getCurrencyRateByName_empty_list() {
        doReturn(Collections.emptyList())
                .when(orderRepository)
                .getCurrencyRateByName(anyString());

        List<CurrencyRateDto> list = orderService.getCurrencyRateByName(TEST_PAIR);

        assertTrue(CollectionUtils.isEmpty(list));

        verify(orderRepository, atLeastOnce()).getCurrencyRateByName(anyString());
    }

    @Test
    public void getClosedOrders_ok() {
        doReturn(Collections.singletonList(OrderDto.builder()
                .currencyPairName(TEST_PAIR)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .dateCreation(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .statusId(3)
                .operationTypeId(4)
                .build()))
                .when(orderRepository)
                .getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());

        List<OrderDto> orders = orderService.getClosedOrders(FROM_DATE, TO_DATE, TEST_PAIR);

        assertFalse(CollectionUtils.isEmpty(orders));
        assertEquals(1, orders.size());

        OrderDto orderDto = orders.get(0);

        assertEquals(TEST_PAIR, orderDto.getCurrencyPairName());
        assertEquals(BigDecimal.TEN, orderDto.getExRate());
        assertEquals(BigDecimal.ONE, orderDto.getAmountBase());
        assertEquals(BigDecimal.TEN, orderDto.getAmountConvert());
        assertEquals(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES), orderDto.getDateAcception());
        assertEquals(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES), orderDto.getDateCreation());
        assertEquals(3, orderDto.getStatusId());
        assertEquals(4, orderDto.getOperationTypeId());

        verify(orderRepository, atLeastOnce()).getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());
    }

    @Test
    public void getClosedOrders_empty_orders_list() {
        doReturn(Collections.emptyList())
                .when(orderRepository)
                .getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());

        List<OrderDto> orders = orderService.getClosedOrders(FROM_DATE, TO_DATE, TEST_PAIR);

        assertTrue(CollectionUtils.isEmpty(orders));

        verify(orderRepository, atLeastOnce()).getClosedOrders(any(LocalDate.class), any(LocalDate.class), anyString());
    }

    @Test
    public void getAllOrders_ok() {
        doReturn(Collections.singletonList(OrderDto.builder()
                .currencyPairName(TEST_PAIR)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .dateCreation(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .statusId(3)
                .operationTypeId(4)
                .build()))
                .when(orderRepository)
                .getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());

        List<OrderDto> orders = orderService.getAllOrders(FROM_DATE.atTime(LocalTime.MIN), TO_DATE.atTime(LocalTime.MAX), TEST_PAIR);

        assertFalse(CollectionUtils.isEmpty(orders));
        assertEquals(1, orders.size());

        OrderDto orderDto = orders.get(0);

        assertEquals(TEST_PAIR, orderDto.getCurrencyPairName());
        assertEquals(BigDecimal.TEN, orderDto.getExRate());
        assertEquals(BigDecimal.ONE, orderDto.getAmountBase());
        assertEquals(BigDecimal.TEN, orderDto.getAmountConvert());
        assertEquals(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES), orderDto.getDateAcception());
        assertEquals(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES), orderDto.getDateCreation());
        assertEquals(3, orderDto.getStatusId());
        assertEquals(4, orderDto.getOperationTypeId());

        verify(orderRepository, atLeastOnce()).getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
    }

    @Test
    public void getAllOrders_empty_orders_list() {
        doReturn(Collections.emptyList())
                .when(orderRepository)
                .getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());

        List<OrderDto> orders = orderService.getAllOrders(FROM_DATE.atTime(LocalTime.MIN), TO_DATE.atTime(LocalTime.MAX), TEST_PAIR);

        assertTrue(CollectionUtils.isEmpty(orders));

        verify(orderRepository, atLeastOnce()).getAllOrders(any(LocalDateTime.class), any(LocalDateTime.class), anyString());
    }
}