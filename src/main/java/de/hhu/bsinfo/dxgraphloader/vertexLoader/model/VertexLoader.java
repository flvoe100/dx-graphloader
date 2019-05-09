package de.hhu.bsinfo.dxgraphloader.vertexLoader.model;

import java.util.HashMap;

public abstract class VertexLoader {

    public HashMap<Long, Integer> idMapper;


    public VertexLoader() {
        this.idMapper = new HashMap<>();
    }


    private void addIdMapping(long externalId, int chunkId) {
        this.idMapper.put(externalId, chunkId);
    }

    public abstract void loadVertices(String filePath);



}
