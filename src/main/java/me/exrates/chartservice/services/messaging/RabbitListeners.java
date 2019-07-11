package me.exrates.chartservice.services.messaging;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.TradeDataService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import static me.exrates.chartservice.configuration.CommonConfiguration.MODULE_MODE_CONSUMES;

@Log4j2
@Profile(MODULE_MODE_CONSUMES)
@Component
public class RabbitListeners {

    private final TradeDataService tradeDataService;

    @Autowired
    public RabbitListeners(TradeDataService tradeDataService) {
        this.tradeDataService = tradeDataService;
    }


    @RabbitListener(queues = "${spring.rabbitmq.tradestopic}")
    public void recieveTrade(TradeDataDto message) {
        System.out.println("income message !!!");
        System.out.println(message);
        /*handle messages here*/
        tradeDataService.handleReceivedTrade(message);
    }
}