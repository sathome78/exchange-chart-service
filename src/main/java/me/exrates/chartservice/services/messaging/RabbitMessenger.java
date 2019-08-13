package me.exrates.chartservice.services.messaging;

import me.exrates.chartservice.model.CandleDetailedDto;

public interface RabbitMessenger {
    void sendNewCandle(CandleDetailedDto dto);
}
