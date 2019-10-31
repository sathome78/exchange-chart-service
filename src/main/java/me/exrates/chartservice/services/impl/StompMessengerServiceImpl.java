package me.exrates.chartservice.services.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.CandleDto;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.StompMessengerService;
import me.exrates.chartservice.utils.PairTransformerUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

import static me.exrates.chartservice.configuration.CommonConfiguration.JSON_MAPPER;

@Log4j2
@Service
public class StompMessengerServiceImpl implements StompMessengerService {

    private final SimpMessagingTemplate messagingTemplate;
    private final ObjectMapper mapper;

    @Autowired
    public StompMessengerServiceImpl(SimpMessagingTemplate messagingTemplate,
                                     @Qualifier(JSON_MAPPER) ObjectMapper mapper) {
        this.messagingTemplate = messagingTemplate;
        this.mapper = mapper;
    }

    @Override
    public void sendLastCandle(CandleModel model, String pairName, BackDealInterval interval) {
        final String transformedPairName = PairTransformerUtil.transformBack(pairName);
        final String resolution = TimeUtil.convertToResolution(interval);

        String destination = String.format("/app/chart/%s/%s", transformedPairName, resolution);

        log.debug("Send last candle chart data to: {}", destination);

        try {
            String message = mapper.writeValueAsString(CandleDto.toDto(model));

            messagingTemplate.convertAndSend(destination, message);
        } catch (Exception ex) {
            log.error(ex);
        }
    }
}