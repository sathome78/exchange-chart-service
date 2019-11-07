package me.exrates.chartservice.services.impl;

import com.antkorwin.xsync.XSync;
import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.converters.CandleDataConverter;
import me.exrates.chartservice.model.DailyDataModel;
import me.exrates.chartservice.model.OrderDataDto;
import me.exrates.chartservice.services.ListenerBuffer;
import me.exrates.chartservice.services.RedisProcessingService;
import me.exrates.chartservice.services.TradeDataService;
import me.exrates.chartservice.utils.RedisGeneratorUtil;
import me.exrates.chartservice.utils.TimeUtil;
import org.apache.commons.lang3.time.StopWatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.isNull;
import static me.exrates.chartservice.configuration.CommonConfiguration.BUFFER_SYNC;

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
        log.info("<<< NEW MESSAGE FROM CORE SERVICE >>> Start processing new data: pair: {}", message.getCurrencyPairName());

        CompletableFuture.runAsync(() -> insertDailyData(message));

        StopWatch stopWatch = StopWatch.createStarted();
        log.debug("<<< BUFFER (RECEIVE AND UPDATE)>>> Start - pair: {}", message.getCurrencyPairName());

        if (isTradeAfterInitializedCandle(message.getCurrencyPairName(), message.getTradeDate())) {
            xSync.execute(message.getCurrencyPairName(), () -> {
                List<OrderDataDto> ordersData = cacheMap.computeIfAbsent(message.getCurrencyPairName(), k -> new ArrayList<>());
                ordersData.add(message);
            });
            Semaphore semaphore = getSemaphoreSafe(message.getCurrencyPairName());
            if (semaphore.tryAcquire()) {
                try {
                    TimeUnit.MILLISECONDS.sleep(BUFFER_DELAY);

                    xSync.execute(message.getCurrencyPairName(), () -> {
                        List<OrderDataDto> ordersData = cacheMap.remove(message.getCurrencyPairName());
                        tradeDataService.handleReceivedTrades(message.getCurrencyPairName(), ordersData);
                    });
                } catch (Exception ex) {
                    log.error(ex);
                } finally {
                    semaphore.release();
                }
            }
        }
        log.debug("<<< BUFFER (RECEIVE AND UPDATE)>>> Finish - pair: {} (time: {}s)", message.getCurrencyPairName(), stopWatch.getTime(TimeUnit.SECONDS));

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

    private boolean isTradeAfterInitializedCandle(String pairName, LocalDateTime tradeDateTime) {
        if (Objects.isNull(tradeDateTime)) {
            return false;
        }

        final LocalDateTime candleDateTime = TimeUtil.getNearestTimeBeforeForMinInterval(tradeDateTime);
        final String hashKey = RedisGeneratorUtil.generateHashKey(pairName);

        LocalDateTime initTime = redisProcessingService.getLastInitializedCandleTimeFromCache(hashKey);
        return isNull(initTime) || initTime.isBefore(candleDateTime) || initTime.isEqual(candleDateTime);
    }

    private void insertDailyData(OrderDataDto orderDataDto) {
        final LocalDateTime candleDateTime = TimeUtil.getNearestTimeBeforeForMinInterval(orderDataDto.getCreateDate());

        final String key = RedisGeneratorUtil.generateKeyForCoinmarketcapData(orderDataDto.getCurrencyPairName());
        final String hashKey = RedisGeneratorUtil.generateHashKeyForCoinmarketcapData(candleDateTime);

        DailyDataModel dailyData = redisProcessingService.getDailyData(key, hashKey);

        BigDecimal currentHighestBid = null;
        BigDecimal currentLowestAsk = null;
        if (Objects.nonNull(dailyData)) {
            final BigDecimal savedHighestBid = dailyData.getHighestBid();
            final BigDecimal savedLowestAsk = dailyData.getLowestAsk();

            if (orderDataDto.getOperationTypeId() == 4) {
                currentHighestBid = CandleDataConverter.getCurrentHighestBid(savedHighestBid, orderDataDto.getExrate());
                currentLowestAsk = savedLowestAsk;
            } else if (orderDataDto.getOperationTypeId() == 3) {
                currentHighestBid = savedHighestBid;
                currentLowestAsk = CandleDataConverter.getCurrentLowestAsk(savedLowestAsk, orderDataDto.getExrate());
            }
        } else {
            if (orderDataDto.getOperationTypeId() == 4) {
                currentHighestBid = orderDataDto.getExrate();
            } else if (orderDataDto.getOperationTypeId() == 3) {
                currentLowestAsk = orderDataDto.getExrate();
            }
        }
        dailyData = new DailyDataModel(candleDateTime, currentHighestBid, currentLowestAsk);

        redisProcessingService.insertDailyData(dailyData, key, hashKey);
    }
}