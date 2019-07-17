package me.exrates.chartservice.configuration;

import lombok.extern.log4j.Log4j2;
import me.exrates.SSMGetter;
import me.exrates.SSMGetterImpl;
import me.exrates.chartservice.properties.SsmProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Log4j2
@Configuration
public class SSMConfiguration {

    private static final String DEV_MODE = "dev";

    @Autowired
    private SsmProperties ssmProperties;

    @Bean
    public SSMGetter ssmGetter() {
        if (ssmProperties.getMode().equals(DEV_MODE)) return new MockSSM();

        return new SSMGetterImpl();
    }

    private class MockSSM implements SSMGetter {
        MockSSM() {
            log.info("Using mock ssm lookup...");
        }

        @Override
        public String lookup(String s) {
//            return "root";
            return "KQmK82dd";
        }
    }
}