package me.exrates.chartservice.configuration;

import com.antkorwin.xsync.XSync;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CommonConfiguration {

    @Bean("jsonMapper")
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