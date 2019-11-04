package me.exrates.chartservice.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import me.exrates.chartservice.model.CandleModel;
import me.exrates.chartservice.services.impl.StompMessengerServiceImpl;
import me.exrates.chartservice.utils.TimeUtil;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.springframework.messaging.simp.SimpMessagingTemplate;

import java.math.BigDecimal;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

public class StompMessengerServiceTest extends AbstractTest {

    @Mock
    private SimpMessagingTemplate messagingTemplate;

    private StompMessengerService stompMessengerService;

    @Before
    public void setUp() throws Exception {
        stompMessengerService = spy(new StompMessengerServiceImpl(messagingTemplate, new ObjectMapper()));
    }

    @Test
    public void sendLastCandle_ok() {
        CandleModel model = CandleModel.builder()
                .pairName(TEST_PAIR)
                .openRate(BigDecimal.TEN)
                .closeRate(BigDecimal.TEN)
                .highRate(BigDecimal.TEN)
                .lowRate(BigDecimal.TEN)
                .volume(BigDecimal.TEN)
                .candleOpenTime(TimeUtil.getNearestBackTimeForBackdealInterval(NOW, M5_INTERVAL))
                .currencyVolume(BigDecimal.TEN)
                .percentChange(BigDecimal.ZERO)
                .valueChange(BigDecimal.ZERO)
                .predLastRate(BigDecimal.ONE)
                .build();

        doNothing()
                .when(messagingTemplate)
                .convertAndSend(anyString(), any(Object.class));

        stompMessengerService.sendLastCandle(model, TEST_PAIR, M5_INTERVAL);

        verify(messagingTemplate, atLeastOnce()).convertAndSend(anyString(), any(Object.class));
    }

    @Test
    public void sendLastCandle_failed() {
        doNothing()
                .when(messagingTemplate)
                .convertAndSend(anyString(), any(Object.class));

        stompMessengerService.sendLastCandle(new CandleModel(), TEST_PAIR, M5_INTERVAL);

        verify(messagingTemplate, never()).convertAndSend(anyString(), any(Object.class));
    }
}