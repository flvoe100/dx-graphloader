package de.hhu.bsinfo.dxgraphloader.metadataLoader.model;

import de.hhu.bsinfo.dxutils.serialization.*;


public class NodesMetaData implements Importable, Exportable {
    private short[] nodesIDs;
    private long[] startVertexIDs;
    private long[] endVertexIDs;
    private int[] numberOfVertices;


    public NodesMetaData() {
    }

    public NodesMetaData(short[] nodesId) {
        this.nodesIDs = nodesId;
        this.startVertexIDs = new long[nodesId.length];
        this.endVertexIDs = new long[nodesId.length];
        this.numberOfVertices = new int[nodesId.length];

    }

    public NodesMetaData(short[] nodeIDs, long[] startVertexIDs, long[] endVertexIDs, int[] numberOfVertices) {
        this.nodesIDs = nodeIDs;
        this.startVertexIDs = startVertexIDs;
        this.endVertexIDs = endVertexIDs;
        this.numberOfVertices = numberOfVertices;
    }


    @Override
    public synchronized void exportObject(Exporter p_exporter) {
        p_exporter.writeShortArray(this.nodesIDs);
        p_exporter.writeLongArray(this.startVertexIDs);
        p_exporter.writeLongArray(this.endVertexIDs);
        p_exporter.writeIntArray(this.numberOfVertices);
    }

    @Override
    public synchronized void importObject(Importer p_importer) {
        p_importer.readShortArray(this.nodesIDs);
        p_importer.readLongArray(this.startVertexIDs);
        p_importer.readLongArray(this.endVertexIDs);
        p_importer.readIntArray(this.numberOfVertices);

    }

    @Override
    public int sizeofObject() {
        return ObjectSizeUtil.sizeofShortArray(this.nodesIDs) + ObjectSizeUtil.sizeofLongArray(this.startVertexIDs) +
                ObjectSizeUtil.sizeofLongArray(this.endVertexIDs) + ObjectSizeUtil.sizeofIntArray(this.numberOfVertices);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.nodesIDs.length; i++) {
            short nodeID = this.nodesIDs[i];
            long numberOfVertices = this.numberOfVertices[i];
            long startVertex = this.startVertexIDs[i];
            long endVertex = this.endVertexIDs[i];
            sb.append(String.format("Nodemetadata for node %d\nNumber of Vertices: %d\nStartvertix: %d\nEndvertix: %d\n", nodeID, numberOfVertices, startVertex, endVertex));
        }

        return sb.toString();
    }
}






