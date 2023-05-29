package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@EnableAsync
public class AsyncService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncService.class);
    private final AppConfig config;
    private final ActiveMQService activeMQService;
    private long ThreadIdealTime = 3000;

    @Autowired
    public AsyncService(AppConfig config, ActiveMQService activeMQService) {
        this.config = config;
        this.activeMQService = activeMQService;
    }

    public CompletableFuture<Void> changeShipper(Map<Integer, String> changesToBeSentToAllWorkers,AtomicInteger superstep, AtomicBoolean resultsGetterDone) {
        return CompletableFuture.runAsync(() -> {
            System.out.println("*DATA SHIPPER*: Edges are received to be shipped to the workers");
            try {
                while (true) {
                    int currentSuperstep = superstep.get();
                    while (!changesToBeSentToAllWorkers.containsKey(currentSuperstep)) {
                        Thread.sleep(ThreadIdealTime);
                        currentSuperstep = superstep.get();
                    }

                    // Wait for the ResultsGetter to update the superstep
                    while (!resultsGetterDone.get()) {
                        Thread.sleep(ThreadIdealTime);
                    }
                    resultsGetterDone.set(false);

                    activeMQService.connectProducer();
                    StringBuilder message;

                    if (changesToBeSentToAllWorkers.containsKey(currentSuperstep)) {
                        message = new StringBuilder();
                        message.append("#change").append("\n").append(changesToBeSentToAllWorkers.get(currentSuperstep));
                        for (String worker : config.getWorkers()) {
                            activeMQService.send(worker, message.toString());
                            System.out.println("*DataShipper*: Change objects have been shared with '" + worker + "' successfully");
                        }
                    }
                    System.out.println("*DataShipper*: All files are shared for the superstep: " + currentSuperstep);
                    changesToBeSentToAllWorkers.remove(currentSuperstep);
                    if (currentSuperstep == config.getTimestamp()) {
                        break;
                    }
                }
                System.out.println("*DataShipper*: All changes are shipped to the workers.");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        });
    }

//    public CompletableFuture<Void> resultsGetter(AtomicInteger superstep, AtomicBoolean resultsGetterDone) {
//        return CompletableFuture.runAsync(() -> {
//            System.out.println("*RESULTS GETTER*: Coordinator listens to get the results back from the workers");
//            for (String worker_name : workersStatus.keySet()) {
//                if (!results.containsKey(worker_name))
//                    results.put(worker_name, new ArrayList<>());
//            }
//            try {
//                while (true) {
//                    activeMQService.connectConsumer("results" + superstep.get());
//
//                    System.out.println("*RESULTS GETTER*: Listening for new messages to get the results " + superstep.get());
//                    String msg = activeMQService.receive();
//                    System.out.println("*RESULTS GETTER*: Received a new message.");
//                    if (msg != null) {
//                        String[] temp = msg.split("@");
//                        if (temp.length == 2) {
//                            String worker_name = temp[0].toLowerCase();
//                            if (workersStatus.containsKey(worker_name)) {
//                                System.out.println("*RESULTS GETTER*: Results received from: '" + worker_name + "'");
//                                results.get(worker_name).add(temp[1]);
//                            } else {
//                                System.out.println("*RESULTS GETTER*: Unable to find the worker name: '" + worker_name + "' in workers list. " +
//                                        "Please update the list in the Config file.");
//                            }
//                        } else {
//                            System.out.println("*RESULTS GETTER*: Message corrupted: " + msg);
//                        }
//                    } else
//                        System.out.println("*RESULTS GETTER*: Error happened. message is null");
//
//                    boolean done = true;
//                    for (String worker_name : workersStatus.keySet()) {
//                        if (results.get(worker_name).size() != superstep.get()) {
//                            done = false;
//                            break;
//                        }
//                    }
//                    if (done) {
//                        superstep.set(superstep.get() + 1);
//                        resultsGetterDone.set(true); // Set the resultsGetterDone to true when superstep is updated
//                        if (superstep.get() > config.getTimestamp()) {
//                            System.out.println("*RESULTS GETTER*: All done! No superstep remained.");
//                            allDone.set(true);
//                            activeMQService.closeConsumer();
//                            break;
//                        }
//                        System.out.println("*RESULTS GETTER*: Starting the new superstep! -> " + superstep.get());
//                    }
//                    activeMQService.closeConsumer();
//                }
//            } catch (Exception e) {
//                e.printStackTrace();
//            }
//        });
//    }

}
