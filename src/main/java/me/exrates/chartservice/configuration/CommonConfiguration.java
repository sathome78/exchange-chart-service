package me.exrates.chartservice.configuration;

import com.antkorwin.xsync.XSync;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommonConfiguration {

    public static final String JSON_MAPPER = "jsonMapper";

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
}