package me.exrates.chartservice.services.messaging;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.ListenerBuffer;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

import static me.exrates.chartservice.configuration.CommonConfiguration.MODULE_MODE_CONSUMES;

@Log4j2
@Profile(MODULE_MODE_CONSUMES)
@Component
@DependsOn("cacheDataInitService")
public class RabbitListeners {

    private final ListenerBuffer listenerBuffer;

    @Autowired
    public RabbitListeners(ListenerBuffer listenerBuffer) {
        this.listenerBuffer = listenerBuffer;
    }

    @RabbitListener(queues = "${spring.rabbitmq.tradestopic}")
    public void receiveTrade(TradeDataDto message) {
        log.debug("received message {}", message);
        listenerBuffer.receive(message);
    }

    @PostConstruct
    private void stop() {
        /*stop consuming here*/
    }
}
