package me.exrates.chartservice.integrations;

import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.time.LocalDateTime;

@RunWith(SpringRunner.class)
@ActiveProfiles("local")
@SpringBootTest
public abstract class AbstractTestIT {

    static final LocalDateTime NOW = LocalDateTime.now();
    static final LocalDateTime FROM_DATE = NOW.minusDays(1);
    static final LocalDateTime TO_DATE = NOW.plusDays(1);
    static final String BTC_USD = "BTC/USD";
}