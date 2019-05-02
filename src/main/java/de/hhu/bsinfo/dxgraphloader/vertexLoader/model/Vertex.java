package de.hhu.bsinfo.dxgraphloader.vertexLoader.model;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;

public class Vertex extends AbstractChunk {

    private long externalId;

    public Vertex(){}

    public Vertex(long externalId) {
        this.externalId = externalId;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(this.externalId);
    }

    @Override
    public void importObject(Importer p_importer) {
        p_importer.readLong(this.externalId);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES;
    }
}
