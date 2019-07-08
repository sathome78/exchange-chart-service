package me.exrates.chartservice.services.messaging;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.TradeDataService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class RabbitListeners {

    private final TradeDataService tradeDataService;

    public RabbitListeners(TradeDataService tradeDataService) {
        this.tradeDataService = tradeDataService;
    }


    @RabbitListener(queues = "${spring.rabbitmq.tradestopic}")
    public void receiveTrade(TradeDataDto message) {
        log.debug("received message {}", message);
        tradeDataService.handleReceivedTrade(message);
    }
}
