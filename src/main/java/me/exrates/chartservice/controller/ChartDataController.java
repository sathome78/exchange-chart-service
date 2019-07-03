package me.exrates.chartservice.controller;

import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

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
