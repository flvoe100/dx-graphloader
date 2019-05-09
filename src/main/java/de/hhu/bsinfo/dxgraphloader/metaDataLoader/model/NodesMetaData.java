package de.hhu.bsinfo.dxgraphloader.metaDataLoader.model;

import de.hhu.bsinfo.dxutils.serialization.*;

public class NodesMetaData implements Importable, Exportable {
    private short[] nodesIDs;
    private long[] startVertexIds;
    private long[] endVertexIds;
    private int[] numberOfVertices;


    public NodesMetaData(short[] nodesId) {
        this.nodesIDs = nodesId;
        this.startVertexIds = new long[nodesId.length];
        this.endVertexIds = new long[nodesId.length];
        this.numberOfVertices = new int[nodesId.length];

    }

    public NodesMetaData(short[] nodeIDs, long[] startVertexIds, long[] endVertexIds, int[] numberOfVertices) {
        this.nodesIDs = nodeIDs;
        this.startVertexIds = startVertexIds;
        this.endVertexIds = endVertexIds;
        this.numberOfVertices = numberOfVertices;
    }

    public void addMetaData(int index, short nodeID, long startVertexID, long endVertexID, int partitionSize) {
        this.nodesIDs[index] = nodeID;
        this.startVertexIds[index] = startVertexID;
        this.endVertexIds[index] = endVertexID;
        this.numberOfVertices[index] = partitionSize;
    }


    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeShortArray(this.nodesIDs);
        p_exporter.writeLongArray(this.startVertexIds);
        p_exporter.writeLongArray(this.endVertexIds);
        p_exporter.writeIntArray(this.numberOfVertices);
    }

    @Override
    public void importObject(Importer p_importer) {
        p_importer.readShortArray(this.nodesIDs);
        p_importer.readLongArray(this.startVertexIds);
        p_importer.readLongArray(this.endVertexIds);
        p_importer.readIntArray(this.numberOfVertices);

    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofShortArray(this.nodesIDs) + ObjectSizeUtil.sizeofLongArray(this.startVertexIds) +
                ObjectSizeUtil.sizeofLongArray(this.endVertexIds) + ObjectSizeUtil.sizeofIntArray(this.numberOfVertices);
    }
}
