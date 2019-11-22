package me.exrates.chartservice.services.messaging;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.OrderDataDto;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.DependsOn;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Log4j2
@Component
@DependsOn({"cacheDataInitService"})
public class RabbitListeners {

    private final TradeDataService tradeDataService;
    private final RedisProcessingService redisProcessingService;
    private final RabbitListenerEndpointRegistry registry;
    private final Environment environment;

    @Autowired
    public RabbitListeners(TradeDataService tradeDataService,
                           RedisProcessingService redisProcessingService,
                           RabbitListenerEndpointRegistry registry,
                           Environment environment) {
        this.tradeDataService = tradeDataService;
        this.redisProcessingService = redisProcessingService;
        this.registry = registry;
        this.environment = environment;
    }

    @RabbitListener(id = "${spring.rabbitmq.tradestopic}", queues = "${spring.rabbitmq.tradestopic}")
    public void receiveTrade(OrderDataDto message) {
        log.info("<<< NEW MESSAGE FROM CORE SERVICE >>> Received message: {}", message);
        //todo: delete this piece of code
        if (Objects.isNull(message.getTradeDate())) {
            return;
        }

        StopWatch stopWatch = StopWatch.createStarted();
        log.info("<<< NEW MESSAGE FROM CORE SERVICE >>> Start processing new data: pair: {}", message.getCurrencyPairName());

        if (isTradeAfterInitializedCandle(message.getCurrencyPairName(), message.getTradeDate())) {
            tradeDataService.handleReceivedTrade(message.getCurrencyPairName(), message);
        }
        log.info("<<< NEW MESSAGE FROM CORE SERVICE >>> End processing new data: pair: {}, trade date: {} (time: {}s)", message.getCurrencyPairName(), message.getTradeDate(), stopWatch.getTime(TimeUnit.SECONDS));
    }

    private boolean isTradeAfterInitializedCandle(String pairName, LocalDateTime tradeDateTime) {
        final LocalDateTime candleDateTime = TimeUtil.getNearestTimeBeforeForMinInterval(tradeDateTime);
        final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

        LocalDateTime initTime = redisProcessingService.getLastInitializedCandleTimeFromCache(hashKey);
        return Objects.isNull(initTime) || initTime.isBefore(candleDateTime) || initTime.isEqual(candleDateTime);
    }

    @PreDestroy
    private void stop() {
        final String property = Objects.requireNonNull(environment.getProperty("spring.rabbitmq.tradestopic"), "Property 'spring.rabbitmq.tradestopic' should not be null");

        if (registry.getListenerContainer(property).isRunning()) {
            registry.stop();
        }
    }
}