package me.exrates.chartservice.controller;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CoinmarketcapApiDto;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.CoinmarketcapService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Log4j2
@RequestMapping(value = "/coinmarketcap", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
@RestController
public class CoinmarketcapController {

    private final CoinmarketcapService coinmarketcapService;

    @Autowired
    public CoinmarketcapController(CoinmarketcapService coinmarketcapService) {
        this.coinmarketcapService = coinmarketcapService;
    }

    @GetMapping("/data")
    public ResponseEntity<List<CoinmarketcapApiDto>> getData(@RequestParam(required = false) String currencyPair,
                                                             @RequestParam IntervalType intervalType,
                                                             @RequestParam int intervalValue) {
        final BackDealInterval interval = new BackDealInterval(intervalValue, intervalType);

        return ResponseEntity.ok(coinmarketcapService.getData(currencyPair, interval));
    }
}