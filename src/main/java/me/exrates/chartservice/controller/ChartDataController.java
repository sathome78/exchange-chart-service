package me.exrates.chartservice.controller;

import lombok.extern.log4j.Log4j2;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import static me.exrates.chartservice.configuration.CommonConfiguration.MODULE_MODE_CONSUMES;

@Log4j2
@Profile(MODULE_MODE_CONSUMES)
@RestController
public class ChartDataController {

    private final Environment env;

    public ChartDataController(Environment env) {
        this.env = env;
    }

    @GetMapping("/status/check")
    public String status()
    {
        return "Working on port " + env.getProperty("local.server.port");
    }

}
