package me.exrates.chartservice.controller;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleDto;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.model.enums.IntervalType;
import me.exrates.chartservice.services.TradeDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;
import java.util.List;

import static java.util.Objects.isNull;
import static java.util.stream.Collectors.toList;

@Log4j2
@RequestMapping(value = "/data", produces = MediaType.APPLICATION_JSON_UTF8_VALUE)
@RestController
public class ChartDataController {

    private final TradeDataService tradeDataService;

    @Autowired
    public ChartDataController(TradeDataService tradeDataService) {
        this.tradeDataService = tradeDataService;
    }


    @GetMapping("/range")
    public ResponseEntity<List<CandleDto>> getRange(@RequestParam String currencyPair,
                                                    @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime from,
                                                    @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,
                                                    @RequestParam IntervalType intervalType,
                                                    @RequestParam int intervalValue) {
        final BackDealInterval interval = new BackDealInterval(intervalValue, intervalType);

        List<CandleDto> response = tradeDataService.getCandles(currencyPair, from, to, interval)
                .stream()
                .map(CandleDto::toDto)
                .collect(toList());

        return ResponseEntity.ok(response);
    }

    @GetMapping("/last")
    public ResponseEntity<CandleDto> getLast(@RequestParam String currencyPair,
                                             @RequestParam IntervalType intervalType,
                                             @RequestParam int intervalValue) {
        final BackDealInterval interval = new BackDealInterval(intervalValue, intervalType);

        CandleModel model = tradeDataService.getCandleForCurrentTime(currencyPair, interval);
        if (isNull(model)) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok(CandleDto.toDto(model));
    }

    @GetMapping("/last-date")
    public ResponseEntity<LocalDateTime> getLastCandleTimeBeforeDate(@RequestParam String currencyPair,
                                                                     @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime to,
                                                                     @RequestParam IntervalType intervalType,
                                                                     @RequestParam int intervalValue) {
        final BackDealInterval interval = new BackDealInterval(intervalValue, intervalType);

        return ResponseEntity.ok(tradeDataService.getLastCandleTimeBeforeDate(currencyPair, to, interval));
    }
}