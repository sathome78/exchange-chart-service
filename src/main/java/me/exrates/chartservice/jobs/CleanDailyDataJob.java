package me.exrates.chartservice.jobs;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.services.CoinmarketcapService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@EnableScheduling
@Log4j2
@Component
public class CleanDailyDataJob {

    private final CoinmarketcapService coinmarketcapService;

    @Autowired
    public CleanDailyDataJob(CoinmarketcapService coinmarketcapService) {
        this.coinmarketcapService = coinmarketcapService;
    }

    @Scheduled(cron = "${scheduled.clean-daily-data}")
    public void cleanDailyData() {
        try {
            coinmarketcapService.cleanDailyData();
        } catch (Exception ex) {
            log.error("--> Job 'cleanDailyData()' occurred error", ex);
        }
    }
}