package com.db.tgfdparallel.process;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.GraphLoader;
import com.db.tgfdparallel.domain.HistogramData;
import com.db.tgfdparallel.service.GraphService;
import com.db.tgfdparallel.service.HistogramService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
public class CoordinatorProcess {
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorProcess.class);
    private final AppConfig config;
    private final GraphService graphService;
    private final HistogramService histogramService;

    @Autowired
    public CoordinatorProcess(AppConfig config, GraphService graphService, HistogramService histogramService) {
        this.config = config;
        this.graphService = graphService;
        this.histogramService = histogramService;
    }

    public synchronized void start() {
        logger.info("Start Coordinator Service");

        Map<String, Integer> fragmentsForTheInitialLoad = graphService.initializeFromSplitGraph(config.getSplitGraphPath());

        String dataPath = config.getDataPath();
        GraphLoader firstLoader = graphService.loadFirstSnapshot(dataPath);
        HistogramData histogramData = histogramService.computeHistogram(firstLoader);

        logger.info("Finish!");
    }

    public void asyncProcess() {

    }
}
