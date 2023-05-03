package com.db.tgfdparallel.service;

import com.db.tgfdparallel.domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

@Service
public class PatternService {

    private static final Logger logger = LoggerFactory.getLogger(PatternService.class);

    public List<PatternTreeNode> vSpawnSinglePatternTreeNode(ProcessedHistogramData histogramData, PatternTree patternTree) {
        addLevel(patternTree);
        List<PatternTreeNode> singleNodePatternTreeNodes = new ArrayList<>();
        List<FrequencyStatistics> sortedVertexHistogram = histogramData.getSortedVertexHistogram();

        System.out.println("VSpawn Level 0");

        for (int i = 0; i < sortedVertexHistogram.size(); i++) {
            long vSpawnTime = System.currentTimeMillis();
            String patternVertexType = sortedVertexHistogram.get(i).getType();

            System.out.println("Vertex type: " + patternVertexType);
            VF2PatternGraph candidatePattern = new VF2PatternGraph();
            Vertex vertex = new Vertex(patternVertexType);
            addVertex(candidatePattern, vertex);

            PatternTreeNode patternTreeNode = new PatternTreeNode();
            patternTreeNode.setPattern(candidatePattern);

            singleNodePatternTreeNodes.add(patternTreeNode);
            patternTree.getTree().get(0).add(patternTreeNode);

            long finalVspawnTime = System.currentTimeMillis() - vSpawnTime;
            logger.info("VSpawn Time: " + finalVspawnTime + " ms");
        }

        System.out.println("GenTree Level 0" + " size: " + patternTree.getTree().get(0).size());

        for (PatternTreeNode node : patternTree.getTree().get(0)) {
            System.out.println("Pattern Type: " + node.getPattern().getCenterVertexType());
        }

        return singleNodePatternTreeNodes;
    }

    public void addLevel(PatternTree tree) {
        tree.getTree().add(new ArrayList<>());
    }

    public void addVertex(VF2PatternGraph candidatePattern, Vertex vertex) {
        candidatePattern.getPattern().addVertex(vertex);
    }

}
