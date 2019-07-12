package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.model.TradeDataDto;
import me.exrates.chartservice.services.ListenerBuffer;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.TimeUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

@Log4j2
@Component
public class ListenerBufferImpl implements ListenerBuffer {

    /*todo: wait for implementation and switch to it*/
    private LocalDateTime lastCandleFromElasticCloseTime = LocalDateTime.now().minusMonths(1);


    private Map<String, List<TradeDataDto>> cacheMap = new ConcurrentHashMap<>();
    private Map<String, Semaphore> synchronizersMap = new ConcurrentHashMap<>();
    private final Object safeSync = new Object();
    private XSync<String> xSync =  new XSync<>();

    private final TradeDataService tradeDataService;

    @Autowired
    public ListenerBufferImpl(TradeDataService tradeDataService) {
        this.tradeDataService = tradeDataService;
    }


    @Override
    public void receive(TradeDataDto message) {
        LocalDateTime thisTradeDate = TimeUtils.getNearestTimeBeforeForMinInterval(message.getTradeDate());
        if (thisTradeDate.isAfter(lastCandleFromElasticCloseTime)) {
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
                        CompletableFuture.runAsync(() -> tradeDataService.handleReceivedTrades(message.getPairName(), trades));
                    });
                } catch (InterruptedException e) {
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
}
