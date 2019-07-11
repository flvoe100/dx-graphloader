package de.hhu.bsinfo.dxgraphloader.edgeLoader.model;

import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.Vertex;

import java.util.HashMap;

public abstract class EdgeLoader {

    public HashMap<Long, Integer> idMapper;
    public LoadingMetaData metaData;
    public Vertex[] vertices;

    public EdgeLoader() {
    }

    public abstract void loadEdges(String filePath,  HashMap<Long, Integer> idMapper, Vertex[] vertices, LoadingMetaData metaData, boolean saveVertex);

    public HashMap<Long, Integer> getIdMapper() {
        return idMapper;
    }

    public void setIdMapper(HashMap<Long, Integer> idMapper) {
        this.idMapper = idMapper;
    }

    public LoadingMetaData getMetaData() {
        return metaData;
    }

    public void setMetaData(LoadingMetaData metaData) {
        this.metaData = metaData;
    }

    public Vertex[] getVertices() {
        return vertices;
    }

    public void setVertices(Vertex[] vertices) {
        this.vertices = vertices;
    }
}
