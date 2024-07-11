package com.db.tgfdparallel.utils;

import com.db.tgfdparallel.domain.*;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeepCopyUtil {
    private static final Logger logger = LoggerFactory.getLogger(DeepCopyUtil.class);
    private static Kryo kryo = new Kryo();

    static {
        kryo.register(VF2DataGraph.class);
        kryo.register(Vertex.class);
        kryo.register(RelationshipEdge.class);
        kryo.register(DefaultDirectedGraph.class, new DefaultDirectedGraphSerializer());
        kryo.register(java.util.HashSet.class);
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(Attribute.class);
        kryo.register(GraphLoader.class);
        kryo.register(VF2PatternGraph.class);
    }

    public static <T> T deepCopy(T original) {
        try {
            Output output = new Output(4096, -1);
            kryo.writeObject(output, original);
            output.close();

            Input input = new Input(output.getBuffer());
            T copy = (T) kryo.readObject(input, original.getClass());
            input.close();
            return copy;
        } catch (Exception e) {
            logger.error("Failed to deep copy object", e);
            return null;
        }
    }
}