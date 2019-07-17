package me.exrates.chartservice.services;

import me.exrates.chartservice.model.TradeDataDto;

public interface ListenerBuffer {

    void receive(TradeDataDto message);

    Boolean isReadyToClose();
}