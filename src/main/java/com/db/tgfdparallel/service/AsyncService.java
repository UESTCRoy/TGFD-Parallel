package com.db.tgfdparallel.service;

import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class AsyncService {

    @Async("taskExecutor")
    public CompletableFuture<Void> setup() {
        // Your setup implementation
        return CompletableFuture.completedFuture(null);
    }

    @Async("taskExecutor")
    public CompletableFuture<Void> shippedDataGenerator() {
        // Your ShippedDataGenerator implementation
        return CompletableFuture.completedFuture(null);
    }

    @Async("taskExecutor")
    public CompletableFuture<Void> jobAssigner() {
        // Your JobAssigner implementation
        return CompletableFuture.completedFuture(null);
    }

    @Async("taskExecutor")
    public CompletableFuture<Void> dataShipper() {
        // Your DataShipper implementation
        return CompletableFuture.completedFuture(null);
    }
}
