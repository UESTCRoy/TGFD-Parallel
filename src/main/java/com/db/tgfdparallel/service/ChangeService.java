package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.Change;
import com.db.tgfdparallel.domain.ChangeType;
import com.db.tgfdparallel.utils.FileUtil;
import org.json.JSONArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Service
public class ChangeService {
    private static final Logger logger = LoggerFactory.getLogger(ChangeService.class);
    private final AppConfig config;
    private final GraphService graphService;

    @Autowired
    public ChangeService(AppConfig config, GraphService graphService) {
        this.config = config;
        this.graphService = graphService;
    }

    public List<List<Change>> changeGenerator() {
        String changeFilePath = config.getChangeFilePath();
        int timestamp = config.getTimestamp();

        return IntStream.range(0, timestamp)
                .mapToObj(i -> {
                    String path = changeFilePath + "/changes_t" + i + "_t" + (i + 1) + "_" + "nospecifictgfds_full" + ".json";
                    JSONArray json = FileUtil.readJsonFile(path);
                    if (json == null) {
                        throw new RuntimeException("Failed to read JSON from file: " + path);
                    } else {
                        List<Change> changes = graphService.loadChanges(json, null, null, true);
                        sortChanges(changes);
                        return changes;
                    }
                })
                .collect(Collectors.toList());
    }

    public static void sortChanges(List<Change> changes) {
        logger.info("Number of changes: " + changes.size());

        Map<ChangeType, Integer> sortOrderMap = new HashMap<ChangeType, Integer>() {{
            put(ChangeType.deleteAttr, 1);
            put(ChangeType.insertAttr, 3);
            put(ChangeType.changeAttr, 1);
            put(ChangeType.deleteEdge, 0);
            put(ChangeType.insertEdge, 3);
            put(ChangeType.changeType, 1);
            put(ChangeType.deleteVertex, 1);
            put(ChangeType.insertVertex, 2);
        }};

        changes.sort(Comparator.comparing(change -> sortOrderMap.get(change.getChangeType())));

        System.out.println("Sorted changes.");
    }

}
