package me.exrates.chartservice.configuration;

import com.google.common.base.Predicates;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.builders.ApiInfoBuilder;
import springfox.documentation.builders.PathSelectors;
import springfox.documentation.builders.RequestHandlerSelectors;
import springfox.documentation.service.ApiInfo;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

import static springfox.documentation.builders.PathSelectors.regex;

@EnableSwagger2
@Configuration
public class SwaggerConfiguration {

    @Value("${swagger.enabled:true}")
    private boolean enabled;

    @Bean
    public Docket api() {
        return new Docket(DocumentationType.SWAGGER_2)
                .enable(enabled)
                .apiInfo(apiInfo())
                .groupName("jsonDoc")
                .select()
                .apis(RequestHandlerSelectors.any())
                .paths(Predicates.not(regex("/access.*")))
                .paths(Predicates.not(regex("/actuator.*")))
                .paths(Predicates.not(regex("/error.*")))
                .paths(PathSelectors.any())
                .build();
    }

    private ApiInfo apiInfo() {
        return new ApiInfoBuilder()
                .title("Chart service")
                .description("Describe REST Web services 'Chart api'.")
                .version("1.0")
                .termsOfServiceUrl("http://terms-of-services.url")
                .license("Licence Type if need")
                .licenseUrl("http://url-to-license.com")
                .build();
    }
}