package me.exrates.chartservice.controller;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.ExchangeRatesDto;
import me.exrates.chartservice.services.DailyDataService;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Log4j2
@RequestMapping(value = "/exchange-rates", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
@RestController
public class ExchangeRatesController {

    private final DailyDataService dailyDataService;

    @Autowired
    public ExchangeRatesController(DailyDataService dailyDataService) {
        this.dailyDataService = dailyDataService;
    }

    @GetMapping
    public ResponseEntity<List<ExchangeRatesDto>> getData(@RequestParam(required = false) String currencyPair,
                                                          @RequestParam String resolution) {
        final BackDealInterval interval = TimeUtil.getInterval(resolution);

        return ResponseEntity.ok(dailyDataService.getExchangeRatesData(currencyPair, interval));
    }
}