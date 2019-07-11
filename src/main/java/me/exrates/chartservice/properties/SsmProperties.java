package me.exrates.chartservice.properties;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

@ConfigurationProperties(prefix = "ssm")
@Data
@Component
public class SsmProperties {

    private String mode;
    private String path;
}