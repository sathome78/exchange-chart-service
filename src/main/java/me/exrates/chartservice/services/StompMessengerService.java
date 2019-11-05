package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;

public interface StompMessengerService {

    void sendLastCandle(CandleModel model, String pairName, BackDealInterval interval);
}