package me.exrates.chartservice.service.messaging;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.TradeDataDto;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class RabbitListeners {


    @RabbitListener(queues = "${spring.rabbitmq.tradestopic}")
    public void worker1(TradeDataDto message) {
        System.out.println("income message !!!");
        System.out.println(message);
        /*handle messages here*/
    }
}
