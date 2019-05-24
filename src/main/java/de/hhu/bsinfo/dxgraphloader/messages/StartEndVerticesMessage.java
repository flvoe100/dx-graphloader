package de.hhu.bsinfo.dxgraphloader.messages;

import de.hhu.bsinfo.dxnet.core.AbstractMessageExporter;
import de.hhu.bsinfo.dxnet.core.AbstractMessageImporter;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxram.DXRAMMessageTypes;

public class StartEndVerticesMessage extends Message {

    private long startExternalVertexID;
    private long startInternalVertexID;
    private long endExternalVertexID;
    private long endInternalVertexID;

    public StartEndVerticesMessage() {
        super();
        this.startExternalVertexID = 0;
        this.startInternalVertexID = 0;
        this.endExternalVertexID = 0;
        this.endInternalVertexID = 0;
    }

    public StartEndVerticesMessage(final short p_destination, long startInternalVertexID, long startExternalVertexID, long endInternalVertexID, long endExternalVertexID) {
        super(p_destination, GraphloadingMessages.GRAPHLOADING_VERTICES_MESSAGE_TYPE, GraphloadingMessages.SUBTYPE_Graphloading_VERTICES_RESPONSE);
        this.startExternalVertexID = startExternalVertexID;
        this.startInternalVertexID = startInternalVertexID;
        this.endExternalVertexID = endExternalVertexID;
        this.endInternalVertexID = endInternalVertexID;
    }


    public long getStartExternalVertexID() {
        return startExternalVertexID;
    }

    public long getEndExternalVertexID() {
        return endExternalVertexID;
    }

    public long getStartInternalVertexID() {
        return startInternalVertexID;
    }

    public long getEndInternalVertexID() {
        return endInternalVertexID;
    }

    @Override
    protected void writePayload(AbstractMessageExporter p_exporter) {
        p_exporter.writeLong(this.startInternalVertexID);
        p_exporter.writeLong(this.startExternalVertexID);
        p_exporter.writeLong(this.endExternalVertexID);
        p_exporter.writeLong(this.endInternalVertexID);
    }

    @Override
    protected void readPayload(AbstractMessageImporter p_importer) {
        this.startInternalVertexID = p_importer.readLong(this.startInternalVertexID);
        this.startExternalVertexID = p_importer.readLong(this.startExternalVertexID);
        this.endExternalVertexID = p_importer.readLong(this.endExternalVertexID);
        this.endInternalVertexID = p_importer.readLong(this.endInternalVertexID);
    }

    @Override
    protected int getPayloadLength() {
        return  Long.BYTES * 4;
    }
}
