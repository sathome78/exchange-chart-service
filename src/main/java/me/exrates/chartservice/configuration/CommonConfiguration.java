package me.exrates.chartservice.configuration;

import com.antkorwin.xsync.XSync;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.enums.IntervalType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Configuration
public class CommonConfiguration {

    public static final BackDealInterval DEFAULT_INTERVAL = new BackDealInterval(5, IntervalType.MINUTE);

    public static final String JSON_MAPPER = "jsonMapper";

    public static final String ALL_SUPPORTED_INTERVALS_LIST = "allSupportedIntervalsList";

    public static final String BUFFER_SYNC = "bufferSync";

    public static final String CANDLES_TOPIC_PREFIX = "candles.";

    @Bean(JSON_MAPPER)
    public ObjectMapper mapper() {
        return new ObjectMapper()
                .findAndRegisterModules()
                .registerModule(new JavaTimeModule());
    }

    @Bean
    public XSync<String> xSync() {
        return new XSync<>();
    }

    @Bean(BUFFER_SYNC)
    public XSync<String> bufferXSync() {
        return new XSync<>();
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertySourcesPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }

    @Bean(ALL_SUPPORTED_INTERVALS_LIST)
    public List<BackDealInterval> getAllSupportedIntervals() {
        return Stream.of(IntervalType.values())
                .map(p -> (IntStream.of(p.getSupportedValues())
                        .mapToObj(v -> new BackDealInterval(v, p))
                        .collect(Collectors.toList())))
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }
}
