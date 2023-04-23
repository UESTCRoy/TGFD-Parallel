package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.Attribute;
import com.db.tgfdparallel.domain.GraphLoader;
import com.db.tgfdparallel.domain.HistogramData;
import com.db.tgfdparallel.domain.Vertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class HistogramService {
    private static final Logger logger = LoggerFactory.getLogger(HistogramService.class);
    private AppConfig config;

    public HistogramService(AppConfig config) {
        this.config = config;
    }

    public HistogramData computeHistogram(GraphLoader firstLoader) {
        HistogramData data = new HistogramData();
        String changeFilePath = config.getChangeFilePath();
        List<String> changePaths = new ArrayList<>();
        for (int i = 0; i < config.getTimestamp(); i++) {
            String path = changeFilePath + "/changes_t" + i + "_t" + (i + 1) + "_" + "nospecifictgfds_full" + ".json";
            changePaths.add(path);
        }

        return data;
    }

    public void readGraphInfo(GraphLoader loader) {
        int edgeCount = loader.getGraph().getGraph().edgeSet().size();
        logger.info("The number of edges in first snapshot " + edgeCount);

        Map<String, Integer> vertexTypesHistogram = new HashMap<>();
        Map<String, Set<Attribute>> vertexTypesToAttributesMap = new HashMap<>();

        for (Vertex v : loader.getGraph().getGraph().vertexSet()) {
            String vertexType = v.getTypes();
            vertexTypesHistogram.merge(vertexType, 1, Integer::sum);
            vertexTypesToAttributesMap.computeIfAbsent(vertexType, k -> new HashSet<>()).addAll(v.getAttributes());


        }
    }
}
