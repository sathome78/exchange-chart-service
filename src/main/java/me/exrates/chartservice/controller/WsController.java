package me.exrates.chartservice.controller;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleDto;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.PairTransformerUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.messaging.handler.annotation.DestinationVariable;
import org.springframework.messaging.simp.annotation.SubscribeMapping;
import org.springframework.stereotype.Controller;

import java.util.Objects;

@Log4j2
@Controller
public class WsController {

    private final TradeDataService tradeDataService;

    @Autowired
    public WsController(TradeDataService tradeDataService) {
        this.tradeDataService = tradeDataService;
    }

    @SubscribeMapping("/chart/{pairName}/{resolution}")
    public CandleDto getLastCandle(@DestinationVariable String pairName, @DestinationVariable String resolution) {
        log.debug("Last candle for: pairName {}, resolution {}", pairName, resolution);

        final String transformedPairName = PairTransformerUtil.transform(pairName);
        final BackDealInterval interval = TimeUtil.getInterval(resolution);

        CandleModel model = tradeDataService.getCandleForCurrentTime(transformedPairName, interval);

        return Objects.isNull(model) ? null : CandleDto.toDto(model);
    }
}