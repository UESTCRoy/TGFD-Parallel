package com.db.tgfdparallel.utils;

import com.db.tgfdparallel.domain.Attribute;
import com.db.tgfdparallel.domain.RelationshipEdge;
import com.db.tgfdparallel.domain.VF2DataGraph;
import com.db.tgfdparallel.domain.Vertex;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import org.jgrapht.graph.DefaultDirectedGraph;

public class DeepCopyUtil {
    private static Kryo kryo = new Kryo();

    static {
        kryo.register(VF2DataGraph.class);
        kryo.register(Vertex.class);
        kryo.register(RelationshipEdge.class);
        kryo.register(DefaultDirectedGraph.class, new DefaultDirectedGraphSerializer());
        kryo.register(java.util.HashSet.class);
        kryo.register(java.util.HashMap.class);
        kryo.register(Attribute.class);
    }

    public static <T> T deepCopy(T original) {
        Output output = new Output(4096, -1);
        kryo.writeObject(output, original);
        output.close();

        Input input = new Input(output.getBuffer());
        T copy = (T) kryo.readObject(input, original.getClass());
        input.close();

        return copy;
    }
}