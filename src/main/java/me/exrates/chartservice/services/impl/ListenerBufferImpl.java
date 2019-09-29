package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.OrderDataDto;
import me.exrates.chartservice.services.ListenerBuffer;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.isNull;
import static me.exrates.chartservice.configuration.CommonConfiguration.BUFFER_SYNC;
import static me.exrates.chartservice.utils.TimeUtil.getNearestTimeBeforeForMinInterval;

@Log4j2
@Component("listenerBuffer")
public class ListenerBufferImpl implements ListenerBuffer {

    private Map<String, List<OrderDataDto>> cacheMap = new ConcurrentHashMap<>();
    private Map<String, Semaphore> synchronizersMap = new ConcurrentHashMap<>();
    private final Object safeSync = new Object();
    private static final Integer BUFFER_DELAY = 1000;

    private final TradeDataService tradeDataService;
    private final RedisProcessingService redisProcessingService;
    private final XSync<String> xSync;

    @Autowired
    public ListenerBufferImpl(TradeDataService tradeDataService,
                              RedisProcessingService redisProcessingService,
                              @Qualifier(BUFFER_SYNC) XSync<String> xSync) {
        this.tradeDataService = tradeDataService;
        this.redisProcessingService = redisProcessingService;
        this.xSync = xSync;
    }

    @Override
    public void receive(OrderDataDto message) {
        log.info("<<< NEW MESSAGE FROM CORE SERVICE >>> Start processing new data: pair: {}, trade date: {}", message.getCurrencyPairName(), message.getTradeDate());

        if (isTradeAfterInitializedCandle(message.getCurrencyPairName(), message)) {
            xSync.execute(message.getCurrencyPairName(), () -> {
                List<OrderDataDto> trades = cacheMap.computeIfAbsent(message.getCurrencyPairName(), k -> new ArrayList<>());
                trades.add(message);
            });
            Semaphore semaphore = getSemaphoreSafe(message.getCurrencyPairName());
            if (semaphore.tryAcquire()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(BUFFER_DELAY);

                    xSync.execute(message.getCurrencyPairName(), () -> {
                        List<OrderDataDto> trades = cacheMap.remove(message.getCurrencyPairName());
                        tradeDataService.handleReceivedTrades(message.getCurrencyPairName(), trades);
                    });
                } catch (Exception ex) {
                    log.error(ex);
                } finally {
                    semaphore.release();
                }
            }
        }
        log.info("<<< NEW MESSAGE FROM CORE SERVICE >>> End processing new data: pair: {}, trade date: {}", message.getCurrencyPairName(), message.getTradeDate());
    }

    private Semaphore getSemaphoreSafe(String pairName) {
        Semaphore semaphore;
        if ((semaphore = synchronizersMap.get(pairName)) == null) {
            synchronized (safeSync) {
                semaphore = synchronizersMap.computeIfAbsent(pairName, k -> new Semaphore(1));
            }
        }
        return semaphore;
    }

    @Override
    public Boolean isReadyToClose() {
        return cacheMap.isEmpty() && synchronizersMap.values().stream().allMatch(p -> p.availablePermits() > 0);
    }

    private boolean isTradeAfterInitializedCandle(String pairName, OrderDataDto orderDataDto) {
        final LocalDateTime orderDateTime = Objects.nonNull(orderDataDto.getTradeDate())
                ? getNearestTimeBeforeForMinInterval(orderDataDto.getTradeDate())
                : getNearestTimeBeforeForMinInterval(orderDataDto.getCreateDate());

        final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

        LocalDateTime initTime = redisProcessingService.getLastInitializedCandleTimeFromCache(hashKey);
        return isNull(initTime) || initTime.isBefore(orderDateTime) || initTime.isEqual(orderDateTime);
    }
}