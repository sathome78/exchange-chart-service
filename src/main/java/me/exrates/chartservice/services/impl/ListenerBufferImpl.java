package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.ListenerBuffer;
import me.exrates.chartservice.services.TradeDataService;
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
import static me.exrates.chartservice.configuration.CommonConfiguration.INIT_TIMES_MAP;
import static me.exrates.chartservice.utils.TimeUtil.getNearestTimeBeforeForMinInterval;

@Log4j2
@Component("listenerBuffer")
public class ListenerBufferImpl implements ListenerBuffer {

    private Map<String, List<TradeDataDto>> cacheMap = new ConcurrentHashMap<>();
    private Map<String, Semaphore> synchronizersMap = new ConcurrentHashMap<>();
    private final Object safeSync = new Object();
    private XSync<String> xSync =  new XSync<>();

    private final TradeDataService tradeDataService;
    private final Map<String, LocalDateTime> initTimesMap;

    @Autowired
    public ListenerBufferImpl(TradeDataService tradeDataService,
                              @Qualifier(INIT_TIMES_MAP) Map<String, LocalDateTime> initTimesMap) {
        this.tradeDataService = tradeDataService;
        this.initTimesMap = initTimesMap;
    }

    @Override
    public void receive(TradeDataDto message) {
        LocalDateTime thisTradeDate = getNearestTimeBeforeForMinInterval(message.getTradeDate());
        if (isTradeAfterInitializedCandle(message.getPairName(), thisTradeDate)) {
            xSync.execute(message.getPairName(), () -> {
                List<TradeDataDto> trades = cacheMap.computeIfAbsent(message.getPairName(), (k) -> new ArrayList<>());
                trades.add(message);
            });
            Semaphore semaphore = getSemaphoreSafe(message.getPairName());
            if (semaphore.tryAcquire()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(1000);
                    xSync.execute(message.getPairName(), () -> {
                        List<TradeDataDto> trades = cacheMap.remove(message.getPairName());
                        tradeDataService.handleReceivedTrades(message.getPairName(), trades);
                    });
                } catch (Exception e) {
                    log.error(e);
                } finally {
                    semaphore.release();
                }
            }
        }
    }

    private Semaphore getSemaphoreSafe(String pairName) {
        Semaphore semaphore;
        if ((semaphore = synchronizersMap.get(pairName)) == null) {
            synchronized (safeSync) {
                semaphore = synchronizersMap.computeIfAbsent(pairName, (k) -> new Semaphore(1));
            }
        }
        return semaphore;
    }

    @Override
    public Boolean isReadyToClose() {
        return cacheMap.isEmpty() && synchronizersMap.values().stream().anyMatch(p -> p.availablePermits() > 0);
    }

    private boolean isTradeAfterInitializedCandle(String pairName, LocalDateTime tradeCandleTime) {
        LocalDateTime initTime = initTimesMap.get(pairName);
        return isNull(initTime) || tradeCandleTime.isAfter(initTime);
    }
}
