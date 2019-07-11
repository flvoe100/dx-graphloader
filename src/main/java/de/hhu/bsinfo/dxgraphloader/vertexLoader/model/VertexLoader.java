package de.hhu.bsinfo.dxgraphloader.vertexLoader.model;


import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;

import java.util.HashMap;

public abstract class VertexLoader {

    public HashMap<Long, Integer> idMapper;
    public Vertex[] vertices = new Vertex[0];
    private int p_index = 0;


    public VertexLoader() {
        this.idMapper = new HashMap<>();
    }

    public VertexLoader(final int size) {
        vertices = new Vertex[size];
    }

    public abstract LoadingMetaData loadVertices(String filePath, LoadingMetaData metaData);

    public void initVertexContainer(final int size) {
        this.vertices = new Vertex[size];
    }

    public boolean addVertex(Vertex v) {
        p_index++;
        if (p_index > vertices.length - 1) {
            return false;
        }

        return true;
    }

    public Vertex[] getVertices() {
        return vertices;
    }

}
