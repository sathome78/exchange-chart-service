package me.exrates.chartservice.controller;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.CandlesDataDto;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.TradeDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

import static me.exrates.chartservice.configuration.CommonConfiguration.MODULE_MODE_CONSUMES;

@Log4j2
@Profile(MODULE_MODE_CONSUMES)
@RequestMapping(value = "/data",
        produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
@RestController
public class ChartDataController {

    private final TradeDataService tradeDataService;

    @Autowired
    public ChartDataController(TradeDataService tradeDataService) {
        this.tradeDataService = tradeDataService;
    }


    @GetMapping("/range")
    public CandlesDataDto getRange(@RequestParam String currencyPair,
                                   @RequestParam LocalDateTime from,
                                   @RequestParam LocalDateTime to,
                                   @RequestParam IntervalType intervalType,
                                   @RequestParam int intervalValue) {
        BackDealInterval backDealInterval = new BackDealInterval(intervalValue, intervalType);
        return tradeDataService.getCandles(currencyPair, from, to, backDealInterval);
    }

    @GetMapping("/last")
    public CandleModel getLast(@RequestParam String currencyPair,
                               @RequestParam IntervalType intervalType,
                               @RequestParam int intervalValue) {
        BackDealInterval backDealInterval = new BackDealInterval(intervalValue, intervalType);
        return tradeDataService.getCandleForCurrentTime(currencyPair, backDealInterval);
    }

}
