package com.db.tgfdparallel.utils;

import com.db.tgfdparallel.domain.RelationshipEdge;
import com.db.tgfdparallel.domain.Vertex;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.jgrapht.graph.DefaultDirectedGraph;

import com.esotericsoftware.kryo.Kryo;

import java.util.HashSet;
import java.util.Set;

public class DefaultDirectedGraphSerializer extends Serializer<DefaultDirectedGraph> {
    @Override
    public void write(Kryo kryo, Output output, DefaultDirectedGraph graph) {
        // Serialize vertices
        Set<Vertex> vertices = graph.vertexSet();
        output.writeInt(vertices.size());
        for (Vertex vertex : vertices) {
            kryo.writeClassAndObject(output, vertex);
        }

        // Serialize edges
        Set<RelationshipEdge> edges = graph.edgeSet();
        output.writeInt(edges.size());
        for (RelationshipEdge edge : edges) {
            kryo.writeClassAndObject(output, edge);
            kryo.writeClassAndObject(output, graph.getEdgeSource(edge));
            kryo.writeClassAndObject(output, graph.getEdgeTarget(edge));
        }
    }

    @Override
    public DefaultDirectedGraph read(Kryo kryo, Input input, Class<? extends DefaultDirectedGraph> type) {
        // Deserialize vertices
        int verticesSize = input.readInt();
        Set<Vertex> vertices = new HashSet<>();
        for (int i = 0; i < verticesSize; i++) {
            vertices.add((Vertex) kryo.readClassAndObject(input));
        }

        // Create graph
        DefaultDirectedGraph graph = new DefaultDirectedGraph<>(RelationshipEdge.class);

        // Add vertices
        for (Vertex vertex : vertices) {
            graph.addVertex(vertex);
        }

        // Deserialize and add edges
        int edgesSize = input.readInt();
        for (int i = 0; i < edgesSize; i++) {
            RelationshipEdge edge = (RelationshipEdge) kryo.readClassAndObject(input);
            Vertex source = (Vertex) kryo.readClassAndObject(input);
            Vertex target = (Vertex) kryo.readClassAndObject(input);
            graph.addEdge(source, target, edge);
        }

        return graph;
    }
}


