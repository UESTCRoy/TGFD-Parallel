package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.Consumer;
import com.db.tgfdparallel.domain.Producer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class ActiveMQService {
    // TODO: 解决什么时候close, singleton实现？
    private static final Logger logger = LoggerFactory.getLogger(ActiveMQService.class);
    private final AppConfig config;
    private final Consumer consumer;
    private final Producer producer;
    private final AtomicBoolean workersStatusChecker;
    private final Map<String, Boolean> workersStatus;

    @Autowired
    public ActiveMQService(AppConfig config, Consumer consumer, Producer producer) {
        this.config = config;
        this.consumer = consumer;
        this.producer = producer;
        this.workersStatusChecker = new AtomicBoolean(true);
        this.workersStatus = new ConcurrentHashMap<>();
    }

    public void initializeWorkersStatus() {
        for (String worker : config.getWorkers()) {
            workersStatus.put(worker, false);
        }
    }

    public void connectProducer() {
        producer.connect();
    }

    public void send(String dstQueue, String messageText) {
        producer.send(dstQueue, messageText);
    }

    public void closeProducer() {
        producer.close();
    }

    // Consumer methods
    public void connectConsumer(String queueName) {
        consumer.connect(queueName);
    }

    public String receive() {
        return consumer.receive();
    }

    public void closeConsumer() {
        consumer.close();
    }

    public void sendMessage(String message) {
        connectProducer();
        for (String worker : config.getWorkers()) {
            send(worker, message);
            logger.info("Message sent to worker: " + worker + " Successfully!");
        }
        closeProducer();
    }

    public void statusCheck() {
        consumer.connect("status");

        while (workersStatusChecker.get()) {
            logger.info("*SETUP*: Listening for new messages to get workers' status...");
            String msg = consumer.receive();
            logger.info("*SETUP*: Received a new message.");

            if (msg != null) {
                if (msg.startsWith("up")) {
                    String[] temp = msg.split(" ");
                    if (temp.length == 2) {
                        String workerName = temp[1];
                        // TODO: workersStatus这里涉及跨越多个class，需要检查与原来代码的区别
                        if (workersStatus.containsKey(workerName)) {
                            logger.info("*SETUP*: Status update: '{}' is up", workerName);
                            workersStatus.put(workerName, true);
                        } else {
                            logger.error("*SETUP*: Unable to find the worker name: '{}' in workers list. Please update the list in the Config file.", workerName);
                        }
                    } else {
                        logger.error("*SETUP*: Message corrupted: {}", msg);
                    }
                } else {
                    logger.error("*SETUP*: Message corrupted: {}", msg);
                }
            } else {
                logger.error("*SETUP*: Error happened.");
            }

            if (workersStatus.values().stream().allMatch(Boolean::booleanValue)) {
                logger.info("*SETUP*: All workers are up and ready to start.");
                workersStatusChecker.set(false);
            }
        }
        consumer.close();
    }

    public void sendStatus() {
        logger.info("Worker '" + config.getNodeName() + "' is up and send status to the Coordinator");
        connectProducer();
        send("status", "up " + config.getNodeName());
        logger.info("Status sent to the Coordinator successfully.");
        closeProducer();
    }

    public void sendResult(int superStepNumber) {
        connectProducer();
        send("results" + superStepNumber, config.getNodeName() + "@" + superStepNumber);
        logger.info("*WORKER*: SupersStep " + superStepNumber + " is done successfully");
        closeProducer();
    }

    public List<String> receiveTGFDsFromWorker(String type) {
        List<String> results = new ArrayList<>();
        try {
            initializeWorkersStatus();
            workersStatusChecker.set(true);
            StringBuilder sb = new StringBuilder();
            String queueName = sb.append(type).append("-tgfd").toString();
            connectConsumer(queueName);
            while (workersStatusChecker.get()) {
                logger.info("*SETUP*: Listening for new messages to get workers' TGFDs");
                String msg = consumer.receive();

                if (msg != null) {
                    String workerName = msg.split("_")[0];
                    if (workersStatus.containsKey(workerName)) {
                        logger.info("{} TGFDs receive from {}", type, workerName);
                        workersStatus.put(workerName, true);

                        if (workersStatus.get(workerName)) {
                            results.add(msg);
                        }
                    } else {
                        logger.error("*SETUP*: Unable to find the worker name: '{}' in workers list. Please update the list in the Config file.", workerName);
                    }
                }

                if (workersStatus.values().stream().allMatch(Boolean::booleanValue)) {
                    logger.info("Received All Workers' TGFDs");
                    workersStatusChecker.set(false);
                }
            }
            closeConsumer();
        } catch (Exception e) {
            logger.error(e.getMessage());
        }
        return results;
    }
}

