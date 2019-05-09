package de.hhu.bsinfo.dxgraphloader.metaDataLoader.model;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class LoadingMetaData extends AbstractChunk {

    private NodesMetaData nodesMetaData;
    private int numOfVertices;
    private int numOfEdges;
    private boolean isDirected;

    public LoadingMetaData() {
    }

    public LoadingMetaData(int numOfVertices, int numOfEdges, boolean isDirected) {
        this.numOfVertices = numOfVertices;
        this.numOfEdges = numOfEdges;
        this.isDirected = isDirected;
    }

    public LoadingMetaData(int numOfVertices, int numOfEdges, short... nodeIds) {
        this.nodesMetaData = new NodesMetaData(nodeIds);
        this.numOfVertices = numOfVertices;
        this.numOfEdges = numOfEdges;
    }

    public boolean isDirected() {
        return isDirected;
    }

    public int getNumOfVertices() {
        return numOfVertices;
    }

    public int getNumOfEdges() {
        return numOfEdges;
    }


    public void setNumOfVertices(int numOfVertices) {
        this.numOfVertices = numOfVertices;
    }

    public void setNumOfEdges(int numOfEdges) {
        this.numOfEdges = numOfEdges;
    }

    public void setDirected(boolean directed) {
        isDirected = directed;
    }

    public void addNodeMetaData(int index, short nodeID, long startVertexID, long endVertexID, int partitionSize) {
        this.nodesMetaData.addMetaData(index, nodeID, startVertexID, endVertexID, partitionSize);
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(this.numOfVertices);
        p_exporter.writeInt(this.numOfEdges);
        p_exporter.writeBoolean(this.isDirected);
        p_exporter.exportObject(this.nodesMetaData);
    }

    @Override
    public void importObject(Importer p_importer) {
        p_importer.readInt(this.numOfVertices);
        p_importer.readInt(this.numOfEdges);
        p_importer.readBoolean(this.isDirected);
        p_importer.importObject(this.nodesMetaData);
    }

    @Override
    public int sizeofObject() {
        return Byte.BYTES + 2 * Integer.BYTES  + this.nodesMetaData.sizeofObject();
    }
}
