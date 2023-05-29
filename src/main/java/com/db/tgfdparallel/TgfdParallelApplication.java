package com.db.tgfdparallel;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.process.CoordinatorProcess;
import com.db.tgfdparallel.process.WorkerProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import java.time.LocalDateTime;

@SpringBootApplication
public class TgfdParallelApplication implements CommandLineRunner {
    private static final Logger logger = LoggerFactory.getLogger(TgfdParallelApplication.class);

    @Autowired
    private CoordinatorProcess coordinatorProcess;

    @Autowired
    private WorkerProcess workerProcess;

    @Autowired
    private AppConfig config;

    public static void main(String[] args) {
        SpringApplication.run(TgfdParallelApplication.class, args);
    }

    @Override
    public void run(String... args) {
        if ("coordinator".equalsIgnoreCase(config.getNodeName())) {
            logger.info("Start Coordinator at {}", LocalDateTime.now());
            coordinatorProcess.start();
            logger.info("Finish Coordinator at {}", LocalDateTime.now());
        } else {
            logger.info("Start {} at {}", config.getNodeName(), LocalDateTime.now());
            workerProcess.start();
            logger.info("Finish {} at {}", config.getNodeName(), LocalDateTime.now());
        }
    }

//    @Bean
//    public CommandLineRunner run(AsyncService asyncService) {
//        return args -> {
//            CompletableFuture<Void> setupTask = asyncService.setup();
//            CompletableFuture<Void> shippedDataTask = asyncService.shippedDataGenerator();
//            CompletableFuture<Void> jobAssignerTask = asyncService.jobAssigner();
//            CompletableFuture<Void> dataShipperTask = asyncService.dataShipper();
//
//            CompletableFuture.allOf(setupTask, shippedDataTask, jobAssignerTask, dataShipperTask).join();
//
//            System.out.println("All tasks completed");
//        };
//    }
}
