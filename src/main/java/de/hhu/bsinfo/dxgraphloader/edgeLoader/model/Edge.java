package de.hhu.bsinfo.dxgraphloader.edgeLoader.model;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;


public class Edge extends AbstractChunk {

    private long sourceVertexID;
    private long destVertexID;

    public Edge(){}

    public Edge(long sourceVertexID, long destVertexID) {
        this.sourceVertexID = sourceVertexID;
        this.destVertexID = destVertexID;
    }

    public long getSourceVertexID() {
        return sourceVertexID;
    }

    public void setSourceVertexID(long sourceVertexID) {
        this.sourceVertexID = sourceVertexID;
    }

    public long getDestVertexID() {
        return destVertexID;
    }

    public void setDestVertexID(long destVertexID) {
        this.destVertexID = destVertexID;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(this.sourceVertexID);
        p_exporter.writeLong(this.destVertexID);

    }

    @Override
    public void importObject(Importer p_importer) {
        this.sourceVertexID = p_importer.readLong(this.sourceVertexID);
        this.destVertexID = p_importer.readLong(this.destVertexID);
    }

    @Override
    public int sizeofObject() {
        return 2 * Long.BYTES;
    }
}
