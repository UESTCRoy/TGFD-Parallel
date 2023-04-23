package com.db.tgfdparallel.service;

import com.db.tgfdparallel.config.AppConfig;
import com.db.tgfdparallel.domain.GraphLoader;
import com.db.tgfdparallel.domain.Vertex;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.riot.RDFDataMgr;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class GraphService {

    private final AppConfig config;
    private final LoaderService loaderService;

    @Autowired
    public GraphService(AppConfig config, LoaderService loaderService) {
        this.config = config;
        this.loaderService = loaderService;
    }

    public Map<String, Integer> initializeFromSplitGraph(List<String> paths) {
        Map<String, Integer> VertexFragment = new HashMap<>();

        for (int i = 0; i < paths.size(); i++) {
            String path = paths.get(i);
            System.out.println("Loading graph from " + path);

            if (!path.toLowerCase().endsWith(".ttl") && !path.toLowerCase().endsWith(".nt")) {
                System.out.println("File format not supported");
                continue;
            }
            Model dataModel = RDFDataMgr.loadModel(path);

            GraphLoader graphLoader = new GraphLoader();
            switch (config.getDataset()) {
                case "dbpedia":
                    graphLoader = loaderService.loadDBPedia(dataModel);
                case "imdb":
//                    loader = new IMDBLoader(new ArrayList<>(), Collections.singletonList(dataModel), Collections.singletonList(dataModel));
                case "synthetic":
//                    loader = new SyntheticLoader(new ArrayList<>(), Collections.singletonList(dataModel), Collections.singletonList(dataModel));
            }

            for (Vertex v : graphLoader.getGraph().getGraph().vertexSet()) {
                String uri = v.getUri();
                VertexFragment.put(uri, i + 1);
            }
        }
        return VertexFragment;
    }

    public GraphLoader loadFirstSnapshot(String path) {
        GraphLoader loader = new GraphLoader();

        Model dataModel = RDFDataMgr.loadModel(path);
        switch (config.getDataset()) {
            case "dbpedia":
                loader = loaderService.loadDBPedia(dataModel);
            case "imdb":
//                    loader = new IMDBLoader(new ArrayList<>(), Collections.singletonList(dataModel), Collections.singletonList(dataModel));
            case "synthetic":
//                    loader = new SyntheticLoader(new ArrayList<>(), Collections.singletonList(dataModel), Collections.singletonList(dataModel));
        }

        return loader;
    }


}