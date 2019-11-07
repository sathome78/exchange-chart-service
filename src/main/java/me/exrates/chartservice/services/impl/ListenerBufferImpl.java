package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.TradeDataDto;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.isNull;
import static me.exrates.chartservice.configuration.CommonConfiguration.BUFFER_SYNC;
import static me.exrates.chartservice.utils.TimeUtil.getNearestTimeBeforeForMinInterval;

@Log4j2
@Component("listenerBuffer")
public class ListenerBufferImpl implements ListenerBuffer {

    private Map<String, List<TradeDataDto>> cacheMap = new ConcurrentHashMap<>();
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
    public void receive(TradeDataDto message) {
        log.info("<<< NEW MESSAGE FROM CORE SERVICE >>> Start processing new data: pair: {}, trade date: {}", message.getPairName(), message.getTradeDate());

        LocalDateTime thisTradeDate = getNearestTimeBeforeForMinInterval(message.getTradeDate());
        if (isTradeAfterInitializedCandle(message.getPairName(), thisTradeDate)) {
//            xSync.execute(message.getPairName(), () -> {
                List<TradeDataDto> trades = cacheMap.computeIfAbsent(message.getPairName(), k -> new ArrayList<>());
                trades.add(message);
//            });
            Semaphore semaphore = getSemaphoreSafe(message.getPairName());
            if (semaphore.tryAcquire()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(BUFFER_DELAY);
//                    xSync.execute(message.getPairName(), () -> {
//                        List<TradeDataDto> trades = cacheMap.remove(message.getPairName());
                        tradeDataService.handleReceivedTrades(message.getPairName(), cacheMap.remove(message.getPairName()));
//                    });
                } catch (Exception ex) {
                    log.error(ex);
                } finally {
                    semaphore.release();
                }
            }
        }
        log.info("<<< NEW MESSAGE FROM CORE SERVICE >>> End processing new data: pair: {}, trade date: {}", message.getPairName(), message.getTradeDate());
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

    private boolean isTradeAfterInitializedCandle(String pairName, LocalDateTime tradeCandleTime) {
        final String key = RedisGeneratorUtil.generateKey(pairName);

        LocalDateTime initTime = redisProcessingService.getLastInitializedCandleTimeFromCache(key);
        return isNull(initTime) || tradeCandleTime.isAfter(initTime);
    }
}
