package me.exrates.chartservice;


import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class ChartdataApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChartdataApplication.class, args);
    }

    @Bean
    public CommandLineRunner job() {
        return new CommandLineRunner() {
            @Override
            public void run(String... args) throws Exception {
                /*todo: initialize by args here*/
            }
        };
    }
}