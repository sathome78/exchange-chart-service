package me.exrates.chartservice.configuration;

import com.antkorwin.xsync.XSync;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import me.exrates.chartservice.model.BackDealInterval;
import me.exrates.chartservice.model.enums.IntervalType;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

@Configuration
public class CommonConfiguration {

    public static final String JSON_MAPPER = "jsonMapper";

    public static final String INIT_TIMES_MAP = "initTimesMap";
    public static final String ALL_SUPPORTED_INTERVALS_LIST = "allSupportedIntervalsList";

    public static final String MODULE_MODE_PRODUCES = "produces";
    public static final String MODULE_MODE_CONSUMES = "consumes";


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

    @Bean(INIT_TIMES_MAP)
    public Map<String, LocalDateTime> lastInitTimesMap() {
        return new HashMap<>();
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