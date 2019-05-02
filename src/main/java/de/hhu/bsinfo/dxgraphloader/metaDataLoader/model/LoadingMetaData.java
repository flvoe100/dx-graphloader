package de.hhu.bsinfo.dxgraphloader.metaDataLoader.model;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

public class LoadingMetaData  extends AbstractChunk {

    private short[] nodeIds;
    private int numOfVertices;
    private int numOfEdges;
    private boolean isDirected;


    public LoadingMetaData(int numOfVertices, int numOfEdges, boolean isDirected) {
        this.numOfVertices = numOfVertices;
        this.numOfEdges = numOfEdges;
        this.isDirected = isDirected;
    }

    public LoadingMetaData(int numOfVertices, int numOfEdges, short... nodeIds) {
        this.nodeIds = nodeIds;
        this.numOfVertices = numOfVertices;
        this.numOfEdges = numOfEdges;
    }

    public void setNodeIds(short[] nodeIds) {
        this.nodeIds = nodeIds;
    }

    public short[] getNodeIds() {
        return nodeIds;
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

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeShortArray(this.nodeIds);
        p_exporter.writeInt(this.numOfVertices);
        p_exporter.writeInt(this.numOfEdges);
        p_exporter.writeBoolean(this.isDirected);
    }

    @Override
    public void importObject(Importer p_importer) {
        p_importer.readShortArray(this.nodeIds);
        p_importer.readInt(this.numOfVertices);
        p_importer.readInt(this.numOfEdges);
        p_importer.readBoolean(this.isDirected);
    }

    @Override
    public int sizeofObject() {
        return Byte.BYTES + 2 * Integer.BYTES + ObjectSizeUtil.sizeofShortArray(this.nodeIds);
    }
}
