package com.db.tgfdparallel.domain;

import lombok.Data;
import lombok.NoArgsConstructor;
import org.jgrapht.Graph;

import java.io.Serializable;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

@Data
@NoArgsConstructor
public class VF2PatternGraph implements Serializable {
    private Graph<Vertex, RelationshipEdge> pattern;
//    private int diameter;
    private String centerVertexType;
    private Vertex centerVertex;
    private PatternType patternType = null;

    public VF2PatternGraph(Graph<Vertex, RelationshipEdge> pattern, String centerVertexType, Vertex centerVertex) {
        this.pattern = pattern;
        this.centerVertexType = centerVertexType;
        this.centerVertex = centerVertex;
    }

    public PatternType getPatternType() {
        return assignPatternType();
    }

    public String getCenterVertexType() {
//        if(centerVertexType.equals("")){
        findCenterNode();
//        }
        return centerVertexType;
    }

    public Vertex getCenterVertex() {
//        if (this.centerVertex == null) {
//            findCenterNode();
//        }
//        return centerVertex;
        return findCenterNode();
    }

    private Vertex findCenterNode() {
        int patternDiameter=0;
        Vertex centerNode=null;
        //k=2
        if(this.pattern.vertexSet().size() == 3 && this.pattern.edgeSet().size() == 2){
            for(Vertex v : this.pattern.vertexSet()){
                if(this.pattern.edgesOf(v).size() == 2){
                    centerNode = v;
                    break;
                }
            }
            assert centerNode != null;
            if(!centerNode.getTypes().isEmpty())
                this.centerVertexType= centerNode.getTypes().iterator().next();
            else
                this.centerVertexType="NoType";
            return centerNode;
        }

        for (Vertex v:this.pattern.vertexSet()) {
            // Define a HashMap to store visited vertices
            HashMap<Vertex,Integer> visited=new HashMap<>();

            // Create a queue for BFS
            LinkedList<Vertex> queue = new LinkedList<>();
            int d=Integer.MAX_VALUE;
            // Mark the current node as visited with distance 0 and then enqueue it
            visited.put(v,0);
            queue.add(v);

            //temp variables
            Vertex x,w;
            while (queue.size() != 0)
            {
                // Dequeue a vertex from queue and get its distance
                x = queue.poll();
                int distance=visited.get(x);
                // Outgoing edges
                for (RelationshipEdge edge : pattern.outgoingEdgesOf(v)) {
                    w = edge.getTarget();
                    // Check if the vertex is not visited
                    if (!visited.containsKey(w)) {
                        // Check if the vertex is within the diameter
                        if (distance + 1 < d) {
                            d = distance + 1;
                        }
                        //Enqueue the vertex and add it to the visited set
                        visited.put(w, distance + 1);
                        queue.add(w);
                    }
                }
                // Incoming edges
                for (RelationshipEdge edge : pattern.incomingEdgesOf(v)) {
                    w = edge.getSource();
                    // Check if the vertex is not visited
                    if (!visited.containsKey(w)) {
                        // Check if the vertex is within the diameter
                        if (distance + 1 < d) {
                            d = distance + 1;
                        }
                        //Enqueue the vertex and add it to the visited set
                        visited.put(w, distance + 1);
                        queue.add(w);
                    }
                }
            }
            if(d>patternDiameter)
            {
                patternDiameter=d;
                centerNode=v;
            }
        }
        assert centerNode != null;
        if(!centerNode.getTypes().isEmpty())
            this.centerVertexType= centerNode.getTypes().iterator().next();
        else
            this.centerVertexType="NoType";
        return centerNode;
    }

    public PatternType assignPatternType() {
        int patternSize = this.getPattern().edgeSet().size();
        if (patternSize < 1){this.patternType=PatternType.SingleNode;}
        else if (patternSize == 1){this.patternType=PatternType.SingleEdge;}
        else if (patternSize == 2){this.patternType=PatternType.DoubleEdge;}
        else if (isStarPattern()){this.patternType=PatternType.Star;}
        else if (isLinePattern()){this.patternType=PatternType.Line;}
        else if (isCirclePattern()){this.patternType=PatternType.Circle;}
        else{this.patternType=PatternType.Complex;}
        return this.patternType;
    }

    private boolean isStarPattern() {
        return this.getPattern().edgesOf(this.getCenterVertex()).size() == this.getPattern().edgeSet().size();
    }

    private boolean isLinePattern() {
        List<Integer> degrees = this.getPattern().vertexSet().stream().map(vertex -> this.getPattern().edgesOf(vertex).size()).collect(Collectors.toList());
        return degrees.stream().filter(degree -> degree == 1).count() == 2 && degrees.stream().filter(degree -> degree == 2).count() == this.getPattern().vertexSet().size() - 2;
    }

    private boolean isCirclePattern() {
        return this.getPattern().vertexSet().stream().allMatch(vertex -> this.getPattern().edgesOf(vertex).size() == 2);
    }
}
