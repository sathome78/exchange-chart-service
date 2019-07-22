package me.exrates.chartservice.services;

import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.enums.IntervalType;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;


@RunWith(SpringRunner.class)
public abstract class AbstractUnitTest {

    static final int CANDLES_TO_STORE = 300;
    static final String BTC_USD = "btc_usd";
    static final String BTC_USDT = "btc_usdt";
    static final BackDealInterval M5_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);
    static final BackDealInterval M15_INTERVAL = new BackDealInterval(15, IntervalType.MINUTE);
    static final BackDealInterval M30_INTERVAL = new BackDealInterval(30, IntervalType.MINUTE);
    static final BackDealInterval ONE_HOUR_INTERVAL = new BackDealInterval(1, IntervalType.HOUR);
    static final BackDealInterval SIX_HOUR_INTERVAL = new BackDealInterval(6, IntervalType.HOUR);
    static final BackDealInterval ONE_DAY_INTERVAL = new BackDealInterval(1, IntervalType.DAY);

}
