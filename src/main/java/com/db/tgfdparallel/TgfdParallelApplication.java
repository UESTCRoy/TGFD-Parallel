package com.db.tgfdparallel;

import com.db.tgfdparallel.service.AsyncService;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import java.util.concurrent.CompletableFuture;

@SpringBootApplication
public class TgfdParallelApplication {

    public static void main(String[] args) {
        SpringApplication.run(TgfdParallelApplication.class, args);
    }

    @Bean
    public CommandLineRunner run(AsyncService asyncService) {
        return args -> {
            CompletableFuture<Void> setupTask = asyncService.setup();
            CompletableFuture<Void> shippedDataTask = asyncService.shippedDataGenerator();
            CompletableFuture<Void> jobAssignerTask = asyncService.jobAssigner();
            CompletableFuture<Void> dataShipperTask = asyncService.dataShipper();

            CompletableFuture.allOf(setupTask, shippedDataTask, jobAssignerTask, dataShipperTask).join();

            System.out.println("All tasks completed");
        };
    }
}
