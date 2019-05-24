package de.hhu.bsinfo.dxgraphloader.vertexLoader.model;


import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;

import java.util.HashMap;

public abstract class VertexLoader {

    public HashMap<Long, Integer> idMapper;


    public VertexLoader() {
        this.idMapper = new HashMap<>();
    }

    public abstract LoadingMetaData loadVertices(String filePath, LoadingMetaData metaData);



}
