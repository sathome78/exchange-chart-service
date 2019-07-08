package me.exrates.chartservice.services;

import me.exrates.chartservice.model.TradeDataDto;

public interface TradeDataService {
    void handleReceivedTrade(TradeDataDto message);
}
