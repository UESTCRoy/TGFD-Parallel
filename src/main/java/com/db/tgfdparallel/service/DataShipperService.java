package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.*;
import com.db.tgfdparallel.utils.FileUtil;
import org.jgrapht.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
public class DataShipperService {
    private static final Logger logger = LoggerFactory.getLogger(DataShipperService.class);
    private final AppConfig config;
    private final ActiveMQService activeMQService;
    private final HDFSService hdfsService;
    private final S3Service s3Service;
    private final AtomicInteger atomicId = new AtomicInteger(0);

    @Autowired
    public DataShipperService(AppConfig config, ActiveMQService activeMQService, HDFSService hdfsService, S3Service s3Service) {
        this.config = config;
        this.activeMQService = activeMQService;
        this.hdfsService = hdfsService;
        this.s3Service = s3Service;
    }

    public Map<Integer, List<String>> dataToBeShippedAndSend(int batchSize, Map<Integer, List<RelationshipEdge>> edgesToBeShipped, Map<String, Integer> fragments) {
        Map<Integer, Map<Integer, List<SimpleEdge>>> batchDataToBeShipped = new HashMap<>();
        Map<Integer, List<String>> listOfFiles = new HashMap<>();

        int count = 0;

        for (Map.Entry<Integer, List<RelationshipEdge>> entry : edgesToBeShipped.entrySet()) {
            int fragmentID = entry.getKey();

            for (RelationshipEdge edge : entry.getValue()) {
                String srcVertex = edge.getSource().getUri();
                String dstVertex = edge.getTarget().getUri();

                Integer srcFragmentID = fragments.get(srcVertex);
                Integer dstFragmentID = fragments.get(dstVertex);

                if (srcFragmentID != null && dstFragmentID != null && !srcFragmentID.equals(dstFragmentID)) {
                    if (!srcFragmentID.equals(fragmentID)) {
                        addEdgeToBatch(batchDataToBeShipped, srcFragmentID, fragmentID, edge);
                    }
                    if (!dstFragmentID.equals(fragmentID)) {
                        addEdgeToBatch(batchDataToBeShipped, dstFragmentID, fragmentID, edge);
                    }
                }

                count++;
                if (count >= batchSize) {
                    sendEdgesToWorkersForShipment(batchDataToBeShipped, listOfFiles);
                    clearBatchData(batchDataToBeShipped);
                    count = 0;
                }
            }
        }

        if (count > 0) {
            sendEdgesToWorkersForShipment(batchDataToBeShipped, listOfFiles);
            clearBatchData(batchDataToBeShipped);
        }
        return listOfFiles;
    }

    private void addEdgeToBatch(Map<Integer, Map<Integer, List<SimpleEdge>>> batchDataToBeShipped, Integer targetFragmentID, Integer fragmentID, RelationshipEdge edge) {
        batchDataToBeShipped
                .computeIfAbsent(targetFragmentID, k -> new HashMap<>())
                .computeIfAbsent(fragmentID, k -> new ArrayList<>())
                .add(new SimpleEdge(edge));
    }

    public void sendEdgesToWorkersForShipment(Map<Integer, Map<Integer, List<SimpleEdge>>> dataToBeShipped, Map<Integer, List<String>> listOfFiles) {
        int currentId = atomicId.incrementAndGet();

        // Initialize listOfFiles
        for (int id : dataToBeShipped.keySet()) {
            if (!listOfFiles.containsKey(id)) {
                listOfFiles.put(id, new ArrayList<>());
            }
        }

        for (int id : dataToBeShipped.keySet()) {
            for (int key : dataToBeShipped.get(id).keySet()) {
                if (dataToBeShipped.get(id).get(key).isEmpty()) {
                    continue;
                }

                StringBuilder sb = new StringBuilder();
                sb.append(key).append("\n");

                for (SimpleEdge edge : dataToBeShipped.get(id).get(key)) {
                    sb.append(edge.getSrc()).append("\t").append(edge.getDst()).append("\t").append(edge.getLabel()).append("\n");
                }

                String fileName = currentId + "_F" + id + "_to_" + key + ".txt";

                if (isAmazonMode()) {
                    s3Service.uploadWholeTextFile(fileName, sb.toString());
                } else {
                    hdfsService.uploadWholeTextFile(config.getHDFSPath(), fileName, sb.toString());
                }

                listOfFiles.get(id).add(fileName);
            }
        }
    }

    private void clearBatchData(Map<Integer, Map<Integer, List<SimpleEdge>>> batchDataToBeShipped) {
        for (int i : batchDataToBeShipped.keySet()) {
            for (int j : batchDataToBeShipped.get(i).keySet()) {
                batchDataToBeShipped.get(i).get(j).clear();
            }
        }
    }

    public void edgeShipper(Map<Integer, List<String>> listOfFiles) {
        logger.info("*DATA SHIPPER*: Edges are received to be shipped to the workers");
        StringBuilder message;
        activeMQService.connectProducer();
        for (int id : listOfFiles.keySet()) {
            message = new StringBuilder();
            message.append("#datashipper").append("\n");
            for (String fileName : listOfFiles.get(id)) {
                message.append(fileName).append("\n");
            }
            activeMQService.send(config.getWorkers().get(id - 1), message.toString());
        }
    }

    public String changeShipped(List<Change> data, int snapshotID) {
        LocalDateTime now = LocalDateTime.now();
        String date = now.getHour() + "_" + now.getMinute() + "_" + now.getSecond();

        String changeFileName = date + "_Change[" + snapshotID + "]" + ".ser";
        if (isAmazonMode()) {
            s3Service.uploadObject(changeFileName, data);
        } else {
            hdfsService.uploadObject(config.getHDFSPath(), changeFileName, data);
        }
        return changeFileName;
    }

    public boolean isAmazonMode() {
        return config.getMode().equals("amazon");
    }

    public void sendHistogramData(ProcessedHistogramData data) {
        if (isAmazonMode()) {
            s3Service.uploadObject("processedHistogramData", data);
        } else {
            hdfsService.uploadObject(config.getHDFSPath(), "processedHistogramData", data);
        }

        activeMQService.sendMessage("#histogramData");
    }

    public ProcessedHistogramData receiveHistogramData() {
        boolean histogramStatsReceived = false;
        ProcessedHistogramData data = null;
        while (!histogramStatsReceived) {
            try {
                activeMQService.connectConsumer(config.getNodeName());
                String msg = activeMQService.receive();
                if (msg != null && msg.startsWith("#histogramData")) {
                    histogramStatsReceived = true;
                    data = isAmazonMode() ?
                            (ProcessedHistogramData) s3Service.downloadObject("processedHistogramData") :
                            (ProcessedHistogramData) hdfsService.downloadObject(config.getHDFSPath(), "processedHistogramData");
                }
                activeMQService.closeConsumer();
            } catch (Exception e) {
                logger.error("Error while receiving histogram data: {}", e.getMessage());
                // No message received, sleep for a while
                try {
                    Thread.sleep(5000); // Sleep for 5 second
                } catch (InterruptedException ie) {
                    // Ignore the interruption and continue the loop
                }
            }
        }
        return data;
    }

    public String uploadSingleNodePattern(List<PatternTreeNode> patternTreeNodes) {
        logger.info("Pattern Tree Nodes: {}", patternTreeNodes);
        String fileName = "SinglePatterns_" + LocalDate.now();
        if (isAmazonMode()) {
            s3Service.uploadObject(fileName, patternTreeNodes);
        } else {
            hdfsService.uploadObject(config.getHDFSPath(), fileName, patternTreeNodes);
        }
        return fileName;
    }

    public List<PatternTreeNode> receiveSinglePatternNode() {
        boolean singlePatternTreeNodesReceived = false;
        List<PatternTreeNode> singlePatternTreeNodesList = new ArrayList<>();

        while (!singlePatternTreeNodesReceived) {
            try {
                activeMQService.connectConsumer(config.getNodeName());
                String msg = activeMQService.receive();
                if (msg != null && msg.startsWith("#singlePattern")) {
                    String fileName = msg.split("\t")[1];
                    if (isAmazonMode()) {
                        singlePatternTreeNodesList = FileUtil.castList(
                                s3Service.downloadObject(fileName), PatternTreeNode.class
                        );
                    } else {
                        singlePatternTreeNodesList = FileUtil.castList(
                                hdfsService.downloadObject(config.getHDFSPath(), fileName), PatternTreeNode.class
                        );
                    }

                    logger.info("All single PatternTreeNodes have been received.");
                    singlePatternTreeNodesReceived = true;
                }
                activeMQService.closeConsumer();
            } catch (IOException e) {
                logger.error("An IOException occurred while receiving single PatternTreeNodes.", e);
            } catch (Exception e) {
                logger.error("An exception occurred while receiving single PatternTreeNodes.", e);
                // An error occurred, sleep for a while
                try {
                    Thread.sleep(5000); // Sleep for 5 second
                } catch (InterruptedException ie) {
                    // Ignore the interruption and continue the loop
                }
            }
        }
        return singlePatternTreeNodesList;
    }

    public Map<Integer, List<SimpleEdge>> readEdgesToBeShipped(String msg) {
        Map<Integer, List<SimpleEdge>> dataToBeShipped = new HashMap<>();
        String[] filePaths = msg.split("\n");

        for (int i = 1; i < filePaths.length; i++) {
            StringBuilder sb;
            if (isAmazonMode()) {
                sb = s3Service.downloadWholeTextFile(filePaths[i]);
            } else {
                sb = hdfsService.downloadWholeTextFile(config.getHDFSPath(), filePaths[i]);
            }

            String[] lines = sb.toString().split("\n");
            int workerID = Integer.parseInt(lines[0]);
            logger.info("*DATA SENDER*: Reading data to be shipped to worker " + config.getWorkers().get(workerID - 1));

            dataToBeShipped.computeIfAbsent(workerID, k -> new ArrayList<>());

            for (int j = 1; j < lines.length; j++) {
                String[] edgeParts = lines[j].split("\t");
                dataToBeShipped.get(workerID).add(new SimpleEdge(edgeParts[0], edgeParts[1], edgeParts[2]));
            }
        }

        return dataToBeShipped;
    }

    public void sendGraphToBeShippedToOtherWorkers(Graph<Vertex, RelationshipEdge> vertexRelationshipEdgeGraph, int workerID) {
        LocalDateTime now = LocalDateTime.now();
        String date = now.getHour() + "_" + now.getMinute() + "_" + now.getSecond();
        String key = date + "_G_" + config.getNodeName() + "_to_" + config.getWorkers().get(workerID - 1) + ".ser";

        if (isAmazonMode()) {
            s3Service.uploadObject(key, vertexRelationshipEdgeGraph);
        } else {
            hdfsService.uploadObject(config.getHDFSPath(), key, vertexRelationshipEdgeGraph);
        }

        activeMQService.connectProducer();
        activeMQService.send(config.getWorkers().get(workerID - 1) + "_data", key);
        activeMQService.closeProducer();
        logger.info("*DATA SENDER*: Graph object has been sent to {} with edge size: {}", config.getWorkers().get(workerID - 1), vertexRelationshipEdgeGraph.edgeSet().size());
    }

    public Object downloadObject(String msg) throws IOException {
        Object obj = null;

        if (isAmazonMode()) {
            obj = s3Service.downloadObject(msg);
        } else {
            obj = hdfsService.downloadObject(config.getHDFSPath(), msg);
        }

        return obj;
    }

    public void uploadTGFD(Map<Integer, List<TGFD>> constantTGFDMap, Map<Integer, List<TGFD>> generalTGFDMap) {
        String constantKey = config.getNodeName() + "_constantTGFD";
        String generalKey = config.getNodeName() + "_generalTGFD";
        if (isAmazonMode()) {
            s3Service.uploadObject(constantKey, constantTGFDMap);
            s3Service.uploadObject(generalKey, generalTGFDMap);
        } else {
            hdfsService.uploadObject(config.getHDFSPath(), constantKey, constantTGFDMap);
            hdfsService.uploadObject(config.getHDFSPath(), generalKey, generalTGFDMap);
        }

        activeMQService.connectProducer();
        activeMQService.send("constant-tgfd", constantKey);
        activeMQService.send("general-tgfd", generalKey);
        logger.info("Worker " + config.getNodeName() + "send TGFDs back to coordinator successfully!");
        activeMQService.closeProducer();
    }

    public Map<Integer, List<TGFD>> downloadConstantTGFD(String type) {
        Map<Integer, List<TGFD>> obj = new HashMap<>();
        try {
            List<String> TGFDsFile = activeMQService.receiveTGFDsFromWorker(type);
            for (String fileName : TGFDsFile) {
                Map<Integer, List<TGFD>> data = (Map<Integer, List<TGFD>>) downloadObject(fileName);
                data.forEach((key, value) ->
                        obj.merge(key, value, (oldValue, newValue) -> {
                            oldValue.addAll(newValue);
                            return oldValue;
                        })
                );
                logger.info("Got {} {} TGFDs from {}", data.size(), type, fileName);
            }
        } catch (IOException e) {
            logger.error("Error while downloading {} TGFD: " + e.getMessage(), type, e);
        } catch (ClassCastException e) {
            logger.error("Error while casting downloaded object to Map<Integer, Set<TGFD>>: " + e.getMessage(), e);
        }
        return obj;
    }

    public List<List<Change>> receiveChangesFromCoordinator() {
        List<List<Change>> changesData = new ArrayList<>();
        boolean changeReceived = false;

        while (!changeReceived) {
            try {
                activeMQService.connectConsumer(config.getNodeName());
                String msg = activeMQService.receive();
                if (msg != null && msg.startsWith("#change")) {
                    String[] lines = msg.split("\n");
                    String[] changeFiles = Arrays.copyOfRange(lines, 1, lines.length);

                    for (String fileName : changeFiles) {
                        Object obj = downloadObject(fileName);
                        if (obj != null) {
                            List<Change> changeList = (List<Change>) obj;
                            changesData.add(changeList);
                            logger.info("Received {} changes from Coordinator", changeList.size());
                        }
                    }
                    changeReceived = true;
                }
                activeMQService.closeConsumer();
            } catch (Exception e) {
                logger.error("Error while receiving changes", e);
                // An error occurred, sleep for a while
                try {
                    Thread.sleep(5000); // Sleep for 5 second
                } catch (InterruptedException ie) {
                    // Ignore the interruption and continue the loop
                }
            }
        }
        return changesData;
    }

    public void awsCoordinatorDataPreparation(List<String> allDataPath, List<String> splitGraphPath, String changeFilePath) {
        logger.info("Downloading Graph Information From S3");
        // Process allDataPath
        processPaths(allDataPath, "/home/ec2-user/completeGraph/");
        // Process splitGraphPath
        processPaths(splitGraphPath, "/home/ec2-user/splitGraph/");
        // Handle changeFile (download entire directory)
        s3Service.downloadObjectsToInstanceDirectory(changeFilePath);
        logger.info("Finish Download From S3 to Instance");
    }

    public String workerDataPreparation() {
        String dataPath = config.getDataPath();
        if (isAmazonMode()) {
            String fileName = getFileNameFromPath(dataPath);
            String destinationFile = "/home/ec2-user/dataPath/" + fileName;
            s3Service.downloadFileToInstance(dataPath, destinationFile);
            return destinationFile;
        } else {
            return dataPath;
        }
    }

    public void processPaths(List<String> paths, String homePath) {
        for (int i = 0; i < paths.size(); i++) {
            String dataPath = paths.get(i);
            String fileName = getFileNameFromPath(dataPath);
            String destinationFile = homePath + fileName;
            s3Service.downloadFileToInstance(dataPath, destinationFile);
            paths.set(i, destinationFile);
        }
    }

    private String getFileNameFromPath(String path) {
        String[] parts = path.split("/");
        return parts[parts.length - 1];
    }

//    public void uploadResultToS3(String fileName) {
//        s3Service.
//    }

}
