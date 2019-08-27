package me.exrates.chartservice.services.messaging;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.ListenerBuffer;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Log4j2
@Component
@DependsOn({"cacheDataInitService", "listenerBuffer"})
public class RabbitListeners {

    private final ListenerBuffer listenerBuffer;
    private final RabbitListenerEndpointRegistry registry;
    private final Environment environment;

    @Autowired
    public RabbitListeners(ListenerBuffer listenerBuffer,
                           RabbitListenerEndpointRegistry registry,
                           Environment environment) {
        this.listenerBuffer = listenerBuffer;
        this.registry = registry;
        this.environment = environment;
    }

    @RabbitListener(id = "${spring.rabbitmq.tradestopic}", queues = "${spring.rabbitmq.tradestopic}")
    public void receiveTrade(TradeDataDto message) {
        log.info("Received message from core service {}", message);

        log.info("Start processing new data: pair name: {}, trade date: {}", message.getPairName(), message.getTradeDate());
        CompletableFuture.runAsync(() -> listenerBuffer.receive(message));
        log.info("End processing new trade data");
    }

    @PreDestroy
    private void stop() {
        final String property = Objects.requireNonNull(environment.getProperty("spring.rabbitmq.tradestopic"), "Property 'spring.rabbitmq.tradestopic' should not be null");

        if (registry.getListenerContainer(property).isRunning()) {
            registry.stop();
        }
        int counter = 0;
        do {
            try {
                Thread.sleep(5000);
                counter++;
            } catch (InterruptedException ex) {
                log.error(ex);
            }
        } while (!listenerBuffer.isReadyToClose() || counter < 10);
    }
}
