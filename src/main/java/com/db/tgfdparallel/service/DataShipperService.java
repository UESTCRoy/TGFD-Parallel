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

@Service
public class DataShipperService {
    private static final Logger logger = LoggerFactory.getLogger(DataShipperService.class);
    private final AppConfig config;
    private final ActiveMQService activeMQService;
    private final HDFSService hdfsService;
    private final S3Service s3Service;

    @Autowired
    public DataShipperService(AppConfig config, ActiveMQService activeMQService, HDFSService hdfsService, S3Service s3Service) {
        this.config = config;
        this.activeMQService = activeMQService;
        this.hdfsService = hdfsService;
        this.s3Service = s3Service;
    }

    public HashMap<Integer, ArrayList<String>> dataToBeShippedAndSend(int batchSize, Map<Integer, List<Job>> jobsByFragmentID, Map<String, Integer> fragments) {
        HashMap<Integer, HashMap<Integer, ArrayList<SimpleEdge>>> batchDataToBeShipped = new HashMap<>();
        HashMap<Integer, ArrayList<String>> listOfFiles = new HashMap<>();

        for (int i : jobsByFragmentID.keySet()) {
            HashMap<Integer, ArrayList<SimpleEdge>> innerMap = new HashMap<>();
            for (int j : jobsByFragmentID.keySet()) {
                if (i != j) {
                    innerMap.put(j, new ArrayList<>());
                }
            }
            batchDataToBeShipped.put(i, innerMap);
        }

        int count = 0;

        for (int fragmentID : jobsByFragmentID.keySet()) {
            for (Job job : jobsByFragmentID.get(fragmentID)) {
                for (RelationshipEdge edge : job.getEdges()) {
                    String srcVertex = edge.getSource().getUri();
                    String dstVertex = edge.getTarget().getUri();

                    if (fragments.get(srcVertex) != fragmentID && fragments.get(dstVertex) == fragmentID) {
                        batchDataToBeShipped.get(fragments.get(srcVertex))
                                .get(fragmentID)
                                .add(new SimpleEdge(edge));
                    } else if (fragments.get(srcVertex) == fragmentID && fragments.get(dstVertex) != fragmentID) {
                        batchDataToBeShipped.get(fragments.get(dstVertex))
                                .get(fragmentID)
                                .add(new SimpleEdge(edge));
                    } else if (fragments.get(srcVertex) != fragmentID && fragments.get(dstVertex) != fragmentID) {
                        batchDataToBeShipped.get(fragments.get(dstVertex))
                                .get(fragmentID)
                                .add(new SimpleEdge(edge));
                        batchDataToBeShipped.get(fragments.get(srcVertex))
                                .get(fragmentID)
                                .add(new SimpleEdge(edge));
                    }

                    count++;

                    if (count >= batchSize) {
                        sendEdgesToWorkersForShipment(batchDataToBeShipped, listOfFiles);
                        clearBatchData(batchDataToBeShipped);
                        count = 0;
                    }
                }
            }
        }

        if (count > 0) {
            sendEdgesToWorkersForShipment(batchDataToBeShipped, listOfFiles);
            clearBatchData(batchDataToBeShipped);
        }
        return listOfFiles;
    }

    // TODO: Solve duplicate fileName date is not enough!
    public void sendEdgesToWorkersForShipment(HashMap<Integer, HashMap<Integer, ArrayList<SimpleEdge>>> dataToBeShipped, HashMap<Integer, ArrayList<String>> listOfFiles) {
        LocalDateTime now = LocalDateTime.now();
        String date = now.getHour() + "_" + now.getMinute() + "_" + now.getNano();

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

                String fileName = date + "_F" + id + "_to_" + key + ".txt";

                if (isAmazonMode()) {
                    s3Service.uploadWholeTextFile(config.getBucketName(), fileName, sb.toString());
                } else {
                    hdfsService.uploadWholeTextFile(config.getHDFSPath(), fileName, sb.toString());
                }

                listOfFiles.get(id).add(fileName);
            }
        }
    }

    private void clearBatchData(HashMap<Integer, HashMap<Integer, ArrayList<SimpleEdge>>> batchDataToBeShipped) {
        for (int i : batchDataToBeShipped.keySet()) {
            for (int j : batchDataToBeShipped.get(i).keySet()) {
                batchDataToBeShipped.get(i).get(j).clear();
            }
        }
    }

    public void edgeShipper(HashMap<Integer, ArrayList<String>> listOfFiles) {
        logger.info("*DATA SHIPPER*: Edges are received to be shipped to the workers");
        StringBuilder message;
        activeMQService.connectProducer();
        for (int id : listOfFiles.keySet()) {
            message = new StringBuilder();
            message.append(id).append("\n");
            for (String fileName : listOfFiles.get(id)) {
                message.append(fileName).append("\n");
            }
            activeMQService.send(config.getWorkers().get(id), message.toString());
        }
    }

    public String changeShipped(List<Change> data, int snapshotID) {
        LocalDateTime now = LocalDateTime.now();
        String date = now.getHour() + "_" + now.getMinute() + "_" + now.getSecond();

        String changeFileName = date + "_Change[" + snapshotID + "]" + ".ser";
        if (isAmazonMode()) {
            s3Service.uploadObject(config.getBucketName(), changeFileName, data);
        } else {
            hdfsService.uploadObject(config.getHDFSPath(), changeFileName, data);
        }
        return changeFileName;
    }

    private boolean isAmazonMode() {
        return config.getMode().equals("amazon");
    }

    public void sendHistogramData(ProcessedHistogramData data) {
        if (isAmazonMode()) {
//            s3Service.uploadObject(config.getBucketName(), "vertexHistogram", data.getVertexHistogram());
//            s3Service.uploadObject(config.getBucketName(), "activeAttributesSet", data.getActiveAttributesSet());
//            s3Service.uploadObject(config.getBucketName(), "vertexTypesToActiveAttributesMap", data.getVertexTypesToActiveAttributesMap());
//            s3Service.uploadObject(config.getBucketName(), "sortedFrequentEdgesHistogram", data.getSortedFrequentEdgesHistogram());
//            s3Service.uploadObject(config.getBucketName(), "sortedVertexHistogram", data.getSortedVertexHistogram());
            s3Service.uploadObject(config.getBucketName(), "processedHistogramData", data);
        } else {
//            hdfsService.uploadObject(config.getHDFSPath(), "vertexHistogram", data.getVertexHistogram());
//            hdfsService.uploadObject(config.getHDFSPath(), "activeAttributesSet", data.getActiveAttributesSet());
//            hdfsService.uploadObject(config.getHDFSPath(), "vertexTypesToActiveAttributesMap", data.getVertexTypesToActiveAttributesMap());
//            hdfsService.uploadObject(config.getHDFSPath(), "sortedFrequentEdgesHistogram", data.getSortedFrequentEdgesHistogram());
//            hdfsService.uploadObject(config.getHDFSPath(), "sortedVertexHistogram", data.getSortedVertexHistogram());
            hdfsService.uploadObject(config.getHDFSPath(), "processedHistogramData", data);
        }

        activeMQService.sendMessage("#histogramData");
    }

    public ProcessedHistogramData receiveHistogramData() {
        boolean histogramStatsReceived = false;
        ProcessedHistogramData data = null;
        while (!histogramStatsReceived) {
            try {
                String msg = activeMQService.receive();
                if (msg != null && msg.startsWith("#histogramData")) {
                    histogramStatsReceived = true;
                    data = isAmazonMode() ?
                            (ProcessedHistogramData) s3Service.downloadObject(config.getBucketName(), "processedHistogramData") :
                            (ProcessedHistogramData) hdfsService.downloadObject(config.getHDFSPath(), "processedHistogramData");
                }
            } catch (Exception e) {
                logger.error("Error while receiving histogram data: {}", e.getMessage());
            }
        }
        return data;
    }

    public String uploadSingleNodePattern(List<PatternTreeNode> patternTreeNodes) {
        logger.info("Pattern Tree Nodes: {}", patternTreeNodes);
        String fileName = "SinglePatterns_" + LocalDate.now();
        if (isAmazonMode()) {
            s3Service.uploadObject(config.getBucketName(), fileName, patternTreeNodes);
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
                String msg = activeMQService.receive();
                if (msg != null && msg.startsWith("#singlePattern")) {
                    String fileName = msg.split("\t")[1];
                    if (isAmazonMode()) {
                        singlePatternTreeNodesList = FileUtil.castList(
                                s3Service.downloadObject(config.getBucketName(), fileName), PatternTreeNode.class
                        );
                    } else {
                        singlePatternTreeNodesList = FileUtil.castList(
                                hdfsService.downloadObject(config.getHDFSPath(), fileName), PatternTreeNode.class
                        );
                    }

                    logger.info("All single PatternTreeNodes have been received.");
                    singlePatternTreeNodesReceived = true;
                }
            } catch (IOException e) {
                logger.error("An IOException occurred while receiving single PatternTreeNodes.", e);
            } catch (Exception e) {
                logger.error("An exception occurred while receiving single PatternTreeNodes.", e);
            }
        }
        return singlePatternTreeNodesList;
    }

    public HashMap<Integer, ArrayList<SimpleEdge>> readEdgesToBeShipped(String msg) {
        HashMap<Integer, ArrayList<SimpleEdge>> dataToBeShipped = new HashMap<>();
        String[] filePaths = msg.split("\n");

        for (int i = 1; i < filePaths.length; i++) {
            StringBuilder sb;
            if (isAmazonMode()) {
                sb = s3Service.downloadWholeTextFile(config.getBucketName(), filePaths[i]);
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
            s3Service.uploadObject(config.getBucketName(), key, vertexRelationshipEdgeGraph);
        } else {
            hdfsService.uploadObject(config.getHDFSPath(), key, vertexRelationshipEdgeGraph);
        }

        activeMQService.connectProducer();
        activeMQService.send(config.getWorkers().get(workerID - 1) + "_data", key);
        activeMQService.closeProducer();
        logger.info("*DATA SENDER*: Graph object has been sent to '" + config.getWorkers().get(workerID - 1) + "' successfully");
    }

    public Object downloadObject(String msg) throws IOException {
        Object obj = null;

        if (isAmazonMode()) {
            obj = s3Service.downloadObject(config.getBucketName(), msg);
        } else {
            obj = hdfsService.downloadObject(config.getHDFSPath(), msg);
        }

        return obj;
    }

    public void uploadConstantTGFD(Map<Integer, Set<TGFD>> constantTGFDMap) {
        String key = config.getNodeName() + "_constantTGFD";
        if (isAmazonMode()) {
            s3Service.uploadObject(config.getBucketName(), key, constantTGFDMap);
        } else {
            hdfsService.uploadObject(config.getBucketName(), key, constantTGFDMap);
        }

        logger.info("Worker " + config.getNodeName() + "finish!");
        activeMQService.connectProducer();
        activeMQService.send("constant-tgfd", config.getNodeName());
        logger.info("Worker " + config.getNodeName() + "send constant tgfds back to coordinator successfully!");
        activeMQService.closeProducer();
    }

    public List<List<Change>> receiveChangesFromCoordinator() {
        List<List<Change>> changesData = new ArrayList<>();
        boolean changeReceived = false;

        while (!changeReceived) {
            try {
                String msg = activeMQService.receive();
                if (msg != null && msg.startsWith("#change")) {
                    String[] lines = msg.split("\n");
                    String[] changeFiles = Arrays.copyOfRange(lines, 1, lines.length);

                    for (String fileName : changeFiles) {
                        Object obj = downloadObject(fileName);
                        if (obj != null) {
                            List<Change> changeList = (List<Change>) obj;
                            changesData.add(changeList);
                        }
                    }
                    changeReceived = true;
                }
            } catch (Exception e) {
                logger.error("Error while receiving changes", e);
            }
        }
        return changesData;
    }
}
