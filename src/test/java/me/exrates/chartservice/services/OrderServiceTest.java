package me.exrates.chartservice.services;

import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.repositories.OrderRepository;
import me.exrates.chartservice.services.impl.OrderServiceImpl;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.util.CollectionUtils;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
    private OrderRepository orderRepository;

    private OrderService orderService;

    @Before
    public void setUp() throws Exception {
        orderService = spy(new OrderServiceImpl(orderRepository));
    }

    @Test
    public void getFilteredOrders_ok() {
        doReturn(Collections.singletonList(OrderDto.builder()
                .id(1)
                .currencyPairName(TEST_PAIR)
                .exRate(BigDecimal.TEN)
                .amountBase(BigDecimal.ONE)
                .amountConvert(BigDecimal.TEN)
                .dateAcception(NOW.atTime(0, 0).plus(10, ChronoUnit.MINUTES))
                .build()))
                .when(orderRepository)
                .getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());

        List<OrderDto> orders = orderService.getFilteredOrders(FROM_DATE, TO_DATE, TEST_PAIR);

        assertFalse(CollectionUtils.isEmpty(orders));
        assertEquals(1, orders.size());

        verify(orderRepository, atLeastOnce()).getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
    }

    @Test
    public void getFilteredOrders_empty_orders_list() {
        doReturn(Collections.emptyList())
                .when(orderRepository)
                .getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());

        List<OrderDto> orders = orderService.getFilteredOrders(FROM_DATE, TO_DATE, TEST_PAIR);

        assertTrue(CollectionUtils.isEmpty(orders));

        verify(orderRepository, atLeastOnce()).getFilteredOrders(any(LocalDate.class), any(LocalDate.class), anyString());
    }
}
