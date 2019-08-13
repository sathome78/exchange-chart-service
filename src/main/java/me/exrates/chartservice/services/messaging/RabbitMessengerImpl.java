package me.exrates.chartservice.services.messaging;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.CandleDetailedDto;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class RabbitMessengerImpl implements RabbitMessenger {

    private final String candlesTopic;
    private final RabbitTemplate rabbitTemplate;

    @Autowired
    public RabbitMessengerImpl(RabbitTemplate rabbitTemplate,
                               @Value("${rabbitmq.candlestopic}") String candlesTopic) {
        this.rabbitTemplate = rabbitTemplate;
        this.candlesTopic = candlesTopic;
    }

    @Override
    public void sendNewCandle(CandleDetailedDto dto) {
        try {
            rabbitTemplate.convertAndSend(candlesTopic, dto);
        } catch (Exception e) {
            log.error("error sending message to redis ", e);
        }
    }
}
