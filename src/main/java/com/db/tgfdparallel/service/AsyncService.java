package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.stereotype.Service;

import javax.jms.JMSException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@EnableAsync
public class AsyncService {

    private static final Logger logger = LoggerFactory.getLogger(AsyncService.class);
    private final AppConfig config;
    private final ActiveMQService activeMQService;

    @Autowired
    public AsyncService(AppConfig config, ActiveMQService activeMQService) {
        this.activeMQService = activeMQService;
    }

    public CompletableFuture<Void> changeShipper(AtomicInteger superstep, AtomicBoolean resultsGetterDone) {
        return CompletableFuture.runAsync(() -> {
            System.out.println("*DATA SHIPPER*: Edges are received to be shipped to the workers");
            try {
                while (true) {
                    int currentSuperstep = superstep.get();
                    while (!changesToBeSentToAllWorkers.containsKey(currentSuperstep)) {
                        Thread.sleep(Config.threadsIdleTime);
                        currentSuperstep = superstep.get();
                    }

                    // Wait for the ResultsGetter to update the superstep
                    while (!resultsGetterDone.get()) {
                        Thread.sleep(Config.threadsIdleTime);
                    }
                    resultsGetterDone.set(false);

                    Producer messageProducer = new Producer();
                    messageProducer.connect();
                    StringBuilder message;

                    if (changesToBeSentToAllWorkers.containsKey(currentSuperstep)) {
                        message = new StringBuilder();
                        message.append("#change").append("\n").append(changesToBeSentToAllWorkers.get(currentSuperstep));
                        for (String worker : Config.workers) {
                            messageProducer.send(worker, message.toString());
                            System.out.println("*DataShipper*: Change objects have been shared with '" + worker + "' successfully");
                        }
                    }
                    messageProducer.close();
                    System.out.println("*DataShipper*: All files are shared for the superstep: " + currentSuperstep);
                    changesToBeSentToAllWorkers.remove(currentSuperstep);
                    if (currentSuperstep == config.getTimestamp()) {
                        break;
                    }
                }
                System.out.println("*DataShipper*: All changes are shipped to the workers.");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (JMSException e) {
                System.out.println("*DataShipper: JMS Exception occurred (DataShipper).  Shutting down coordinator.");
            }
        });
    }

    public CompletableFuture<Void> resultsGetter(AtomicInteger superstep, AtomicBoolean resultsGetterDone) {
        return CompletableFuture.runAsync(() -> {
            System.out.println("*RESULTS GETTER*: Coordinator listens to get the results back from the workers");
            for (String worker_name : workersStatus.keySet()) {
                if (!results.containsKey(worker_name))
                    results.put(worker_name, new ArrayList<>());
            }
            try {
                while (true) {
                    Consumer consumer = new Consumer();
                    consumer.connect("results" + superstep.get());

                    System.out.println("*RESULTS GETTER*: Listening for new messages to get the results " + superstep.get());
                    String msg = consumer.receive();
                    System.out.println("*RESULTS GETTER*: Received a new message.");
                    if (msg != null) {
                        String[] temp = msg.split("@");
                        if (temp.length == 2) {
                            String worker_name = temp[0].toLowerCase();
                            if (workersStatus.containsKey(worker_name)) {
                                System.out.println("*RESULTS GETTER*: Results received from: '" + worker_name + "'");
                                results.get(worker_name).add(temp[1]);
                            } else {
                                System.out.println("*RESULTS GETTER*: Unable to find the worker name: '" + worker_name + "' in workers list. " +
                                        "Please update the list in the Config file.");
                            }
                        } else {
                            System.out.println("*RESULTS GETTER*: Message corrupted: " + msg);
                        }
                    } else
                        System.out.println("*RESULTS GETTER*: Error happened. message is null");

                    boolean done = true;
                    for (String worker_name : workersStatus.keySet()) {
                        if (results.get(worker_name).size() != superstep.get()) {
                            done = false;
                            break;
                        }
                    }
                    if (done) {
                        superstep.set(superstep.get() + 1);
                        resultsGetterDone.set(true); // Set the resultsGetterDone to true when superstep is updated
                        if (superstep.get() > config.getTimestamp()) {
                            System.out.println("*RESULTS GETTER*: All done! No superstep remained.");
                            allDone.set(true);
                            consumer.close();
                            break;
                        }
                        System.out.println("*RESULTS GETTER*: Starting the new superstep! -> " + superstep.get());
                    }
                    consumer.close();
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
    }


//    @Async
//    public void changeShipper(AtomicInteger superstep) {
//        // Place your ChangeShipper code here.
//        // Access superstep directly since it's passed as a parameter.
//        // ...
//    }
//
//    @Async
//    public void resultsGetter(AtomicInteger superstep) {
//        // Place your ResultsGetter code here.
//        // Access superstep directly since it's passed as a parameter.
//        // ...
//    }
}
