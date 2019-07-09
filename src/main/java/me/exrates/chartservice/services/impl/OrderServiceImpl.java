package me.exrates.chartservice.services.impl;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.OrderDto;
import me.exrates.chartservice.repositories.OrderRepository;
import me.exrates.chartservice.services.OrderService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

@Log4j2
@Service
public class OrderServiceImpl implements OrderService {

    private final OrderRepository orderRepository;

    @Autowired
    public OrderServiceImpl(OrderRepository orderRepository) {
        this.orderRepository = orderRepository;
    }

    @Override
    public List<OrderDto> getFilteredOrders(LocalDate fromDate, LocalDate toDate, String pairName) {
        return orderRepository.getFilteredOrders(fromDate, toDate, pairName);
    }
}