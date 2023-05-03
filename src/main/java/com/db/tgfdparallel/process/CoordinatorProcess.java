package com.db.tgfdparallel.process;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class CoordinatorProcess {
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorProcess.class);
    private final AppConfig config;
    private final GraphService graphService;
    private final HistogramService histogramService;
    private final PatternService patternService;
    private final ActiveMQService activeMQService;
    private final JobService jobService;
    private final DataShipperService dataShipperService;
    private final ChangeService changeService;
    private final AsyncService asyncService;

    @Autowired
    public CoordinatorProcess(AppConfig config, GraphService graphService, HistogramService histogramService, PatternService patternService,
                              ActiveMQService activeMQService, JobService jobService, DataShipperService dataShipperService, ChangeService changeService,
                              AsyncService asyncService) {
        this.config = config;
        this.graphService = graphService;
        this.histogramService = histogramService;
        this.patternService = patternService;
        this.activeMQService = activeMQService;
        this.jobService = jobService;
        this.dataShipperService = dataShipperService;
        this.changeService = changeService;
        this.asyncService = asyncService;
    }

    public void dataPreparation() {
        logger.info("Data preparation started at {}", LocalDate.now());

        logger.info("Check the status of the workers");
        activeMQService.statusCheck();

        // Generate all the changes for histogram computation and sending to the worker
        List<List<Change>> changesData = changeService.chagneGenerator();

        // Send the histogram data to the worker
        String dataPath = config.getDataPath();
        logger.info("Load the first snapshot from the data path: {}", dataPath);
        GraphLoader firstLoader = graphService.loadFirstSnapshot(dataPath);
        ProcessedHistogramData histogramData = histogramService.computeHistogram(firstLoader, changesData);
        logger.info("Send the histogram data to the worker");
        dataShipperService.sendHistogramData(histogramData);

        // First Level initialization of the pattern tree
        PatternTree patternTree = new PatternTree();
        List<PatternTreeNode> patternTreeNodes = patternService.vSpawnSinglePatternTreeNode(histogramData, patternTree);

        // Send the first level of pattern tree to the worker
        String fileName = dataShipperService.uploadSingleNodePattern(patternTreeNodes);

        // Send single pattern tree to the worker
        logger.info("Send single pattern tree to the worker");
        activeMQService.sendMessage("#singlePattern" + "\t" + fileName);

        // Initialize the graph from the split graph, String (VertexURI) -> Integer (FragmentID)
        Map<String, Integer> fragmentsForTheInitialLoad = graphService.initializeFromSplitGraph(config.getSplitGraphPath());

        // Define jobs and assign them to the workers
        Map<Integer, List<Job>> jobsByFragmentID = jobService.defineJobs(firstLoader, fragmentsForTheInitialLoad, patternTreeNodes);
        jobService.jobAssigner(jobsByFragmentID);

        // Send the edge data to the workers
        HashMap<Integer, ArrayList<String>> listOfFiles = dataShipperService.dataToBeShippedAndSend(800000, jobsByFragmentID, fragmentsForTheInitialLoad);
        dataShipperService.edgeShipper(listOfFiles);

        // Send the changes to the workers
        Map<Integer, String> changesToBeSentToAllWorkers = new HashMap<>();
        for (int i = 0; i < changesData.size(); i++) {
            String changeFileName = dataShipperService.changeShipped(changesData.get(i), i + 2);
            changesToBeSentToAllWorkers.put(i + 2, changeFileName);
        }

        changeShipperAndWaitResult();
    }

    public void changeShipperAndWaitResult() {
        AtomicInteger superstep = new AtomicInteger(1);
        CountDownLatch latch = new CountDownLatch(1);

        CompletableFuture<Void> changeShipperFuture = asyncService.changeShipper(superstep, latch);
        CompletableFuture<Void> resultsGetterFuture = asyncService.resultsGetter(superstep, latch);

        // Wait for both methods to complete
        CompletableFuture.allOf(changeShipperFuture, resultsGetterFuture).join();
    }

//    public void changeShipperAndWaitResult() {
//        AtomicInteger superstep = new AtomicInteger(1);
//
//        asyncService.changeShipper(superstep);
//        asyncService.resultsGetter(superstep);
//
//        // You can add additional logic here if needed.
//    }

}
