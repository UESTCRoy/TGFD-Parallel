package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import org.jgrapht.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
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

    public Map<Integer, List<Job>> defineJobs(Graph<Vertex, RelationshipEdge> graph, Map<String, Integer> fragmentsForTheInitialLoad, List<PatternTreeNode> singlePatternTreeNodes) {
        Map<Integer, List<Job>> jobsByFragmentID = new HashMap<>();
        AtomicInteger jobID = new AtomicInteger(0);
        int diameter = 2;
        IntStream.rangeClosed(1, config.getWorkers().size())
                .forEach(i -> jobsByFragmentID.put(i, new ArrayList<>()));

        // TODO: The fragmentID of the job may not be useful ant all!
        for (PatternTreeNode ptn : singlePatternTreeNodes) {
            String centerNodeType = ptn.getPattern().getCenterVertexType();
            graph.vertexSet().stream()
                    .filter(v -> v.getTypes().contains(centerNodeType))
                    .forEach(v -> {
                        int currentJobID = jobID.incrementAndGet();
                        List<RelationshipEdge> edges = graphService.getEdgesWithinDiameter(graph, v, diameter);
                        // TODO: 这里的字段似乎只需要edges，DataShipperService的dataToBeShippedAndSend
                        Job job = new Job(currentJobID, diameter, v, fragmentsForTheInitialLoad.get(v.getUri()), edges, ptn);
                        jobsByFragmentID.get(fragmentsForTheInitialLoad.get(v.getUri())).add(job);
                        if (currentJobID % 100 == 0) {
                            logger.info("Jobs so far: {}  **  {}", currentJobID, LocalDateTime.now());
                        }
                    });
        }
        return jobsByFragmentID;
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
                    .filter(v -> v.getTypes().contains(centerNodeType))
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

    // TODO: we might not need this function, because worker doesn't need jobs, it could only based on singleNodePattern
    public void jobAssigner(Map<Integer, List<Job>> jobsByFragmentID) {
        logger.info("*JOB ASSIGNER*: Jobs are received to be assigned to the workers");

        StringBuilder message;
        activeMQService.connectProducer();

        for (int workerID : jobsByFragmentID.keySet()) {
            message = new StringBuilder();
            message.append("#jobs").append("\n");
            for (Job job : jobsByFragmentID.get(workerID)) {
                // A job is in the form of the following
                // id # CenterNodeVertexID # diameter # FragmentID # Type
                message.append(job.getID()).append("#")
                        .append(job.getCenterNode().getUri()).append("#")
                        .append(job.getDiameter()).append("#")
                        .append(job.getFragmentID()).append("#")
                        .append(job.getCenterNode().getTypes())
                        .append("\n");
            }

            activeMQService.send(config.getWorkers().get(workerID - 1), message.toString());
            logger.info("*JOB ASSIGNER*: jobs assigned to '{}' successfully", config.getWorkers().get(workerID - 1));
        }

        activeMQService.closeProducer();
        logger.info("*JOB ASSIGNER*: All jobs are assigned.");
    }

    public Map<Integer, Set<Job>> createNewJobsSet(Map<Integer, Set<Job>> assignedJobsBySnapshot, VF2PatternGraph pattern, PatternTreeNode newPattern) {
        Map<Integer, Set<Job>> newJobsSet = new HashMap<>();
        for (int index : assignedJobsBySnapshot.keySet()) {
            Set<Job> newJobsAtIndex = new HashSet<>();
            Set<Job> additionalJobs = new HashSet<>();
            for (Job job : assignedJobsBySnapshot.get(index)) {
                if (job.getPatternTreeNode().getPattern().equals(pattern)) {
                    Job newJob = new Job(job.getID(), job.getCenterNode(), newPattern);
                    newJobsAtIndex.add(newJob);
                    additionalJobs.add(newJob);
                }
            }
            assignedJobsBySnapshot.get(index).addAll(additionalJobs);
            newJobsSet.put(index, newJobsAtIndex);
        }
        return newJobsSet;
    }

}
