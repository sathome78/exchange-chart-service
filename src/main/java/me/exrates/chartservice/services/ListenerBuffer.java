package me.exrates.chartservice.services;

import me.exrates.chartservice.model.OrderDataDto;

public interface ListenerBuffer {

    void receive(OrderDataDto message);

    Boolean isReadyToClose();
}