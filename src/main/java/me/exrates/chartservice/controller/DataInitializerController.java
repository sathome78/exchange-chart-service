package me.exrates.chartservice.controller;

import lombok.extern.log4j.Log4j2;
import me.exrates.chartservice.services.DataInitializerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

@Log4j2
@RequestMapping("/data-initializer")
@RestController
public class DataInitializerController {

    private final DataInitializerService initializerService;

    @Autowired
    public DataInitializerController(DataInitializerService initializerService) {
        this.initializerService = initializerService;
    }

    @PostMapping("/generate")
    public ResponseEntity generateDataByCriteria(@RequestParam(value = "from", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
                                                 @RequestParam(value = "to", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate,
                                                 @RequestParam("pairs") List<String> pairs) {
        log.info("Generate for pairs: {} - from: {}, to: {}", pairs.toString(), fromDate, toDate);

        initializerService.generate(fromDate, toDate, pairs);

        log.info("Process of generation cache data for: {} is DONE!", pairs.toString());

        return ResponseEntity.ok().build();
    }

    @PostMapping("/generate/all")
    public ResponseEntity generateAllData(@RequestParam(value = "from", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate fromDate,
                                          @RequestParam(value = "to", required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate toDate) {
        log.info("Generate for all pairs - from: {}, to: {}", fromDate, toDate);

        initializerService.generate(fromDate, toDate);

        log.info("Process of generation cache data for: ALL is DONE!");

        return ResponseEntity.ok().build();
    }
}