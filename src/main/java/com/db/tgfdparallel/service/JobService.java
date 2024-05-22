package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import org.jgrapht.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class JobService {
    private static final Logger logger = LoggerFactory.getLogger(JobService.class);
    private final AppConfig config;
    private final GraphService graphService;
    private final ActiveMQService activeMQService;

    @Autowired
    public JobService(AppConfig config, GraphService graphService, ActiveMQService activeMQService) {
        this.config = config;
        this.graphService = graphService;
        this.activeMQService = activeMQService;
    }

    // Record which partition each vertex is assigned to, and assign edges accordingly.
    public Map<Integer, List<RelationshipEdge>> defineEdgesToBeShipped(Graph<Vertex, RelationshipEdge> graph, Map<String, Integer> fragmentsForTheInitialLoad,
                                                                       List<PatternTreeNode> singlePatternTreeNodes) {
        Map<Integer, List<RelationshipEdge>> edgesInfo = new HashMap<>();
        IntStream.rangeClosed(1, config.getWorkers().size())
                .forEach(i -> edgesInfo.put(i, new ArrayList<>()));
        int diameter = 1;
        AtomicInteger count = new AtomicInteger(0);

        for (PatternTreeNode ptn : singlePatternTreeNodes) {
            String centerNodeType = ptn.getPattern().getCenterVertexType();
            graph.vertexSet().stream()
                    .filter(v -> v.getType().equals(centerNodeType))
                    .forEach(v -> {
                        List<RelationshipEdge> edges = graphService.getEdgesWithinDiameter(graph, v, diameter);
                        int fragmentID = fragmentsForTheInitialLoad.getOrDefault(v.getUri(), 0);
                        if (fragmentID != 0) {
                            edgesInfo.get(fragmentID).addAll(edges);
                        } else {
                            count.getAndIncrement();
                        }
                    });
        }
        for (Map.Entry<Integer, List<RelationshipEdge>> entry : edgesInfo.entrySet()) {
            logger.info("At {} we have {} edges to be shipped", entry.getKey(), entry.getValue().size());
        }
        logger.info("When we try to send edges to all worker, we found there are {} missing edges from splitGraph!!!", count);

        return edgesInfo;
    }

    public List<List<Job>> createNewJobsSet(List<List<Job>> previousLevelJobList, VF2PatternGraph pattern, PatternTreeNode newPattern) {
        List<List<Job>> currentLevelJobList = new ArrayList<>(config.getTimestamp());

        for (List<Job> previousJobs : previousLevelJobList) {
            List<Job> filteredJobs = previousJobs.stream()
                    .filter(job -> job.getPatternTreeNode().getPattern().equals(pattern))
                    .map(job -> new Job(job.getCenterNode(), newPattern))
                    .collect(Collectors.toList());

            currentLevelJobList.add(filteredJobs);
        }

        return currentLevelJobList;
    }

}
