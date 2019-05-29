package de.hhu.bsinfo.dxgraphloader.metaDataLoader.model;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;

public class LoadingMetaData extends AbstractChunk {

    private final Logger LOGGER = LogManager.getFormatterLogger(LoadingMetaData.class);

    private int numOfVertices = 0;
    private int numOfEdges = 0;
    private int isDirected = 0;

    private short[] nodesIDs;

    private long[] startExternalVertexIDs;
    private long[] startInternalVertexIDs;

    private long[] endExternalVertexIDs;
    private long[] endInternalVertexIDs;

    private int[] numberOfVerticesOfNodes;

    public LoadingMetaData(int numberOfNodes) {
        this.nodesIDs = new short[numberOfNodes];

        this.startExternalVertexIDs = new long[numberOfNodes];
        this.startInternalVertexIDs = new long[numberOfNodes];

        this.endExternalVertexIDs = new long[numberOfNodes];
        this.endInternalVertexIDs = new long[numberOfNodes];

        this.numberOfVerticesOfNodes = new int[numberOfNodes];

        Arrays.fill(this.nodesIDs, (short) 0);

        Arrays.fill(this.startExternalVertexIDs, 0);
        Arrays.fill(this.startInternalVertexIDs, 0);

        Arrays.fill(this.endExternalVertexIDs, 0);
        Arrays.fill(this.endInternalVertexIDs, 0);

        Arrays.fill(this.numberOfVerticesOfNodes, 0);
    }

    public LoadingMetaData(short[] nodeIDs) {
        this.nodesIDs = nodeIDs;

        this.startExternalVertexIDs = new long[nodeIDs.length];
        this.startInternalVertexIDs = new long[nodeIDs.length];

        this.endExternalVertexIDs = new long[nodeIDs.length];
        this.endInternalVertexIDs = new long[nodeIDs.length];

        this.numberOfVerticesOfNodes = new int[nodeIDs.length];

        Arrays.fill(this.startExternalVertexIDs, 0);
        Arrays.fill(this.startInternalVertexIDs, 0);

        Arrays.fill(this.endExternalVertexIDs, 0);
        Arrays.fill(this.endInternalVertexIDs, 0);

        Arrays.fill(this.numberOfVerticesOfNodes, 0);
    }

    public int getNumberOfNodes() {
        return this.nodesIDs.length;
    }

    public boolean isDirected() {
        return this.isDirected == 1;
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
        isDirected = directed ? 1 : 0;
    }

    public void addNodeMetaData(int index, short nodeID, long startVertexID, long endVertexID, int partitionSize) {
        this.nodesIDs[index] = nodeID;
        this.startExternalVertexIDs[index] = startVertexID;
        this.endExternalVertexIDs[index] = endVertexID;
        this.numberOfVerticesOfNodes[index] = partitionSize;
    }

    public int getNumOfVerticesOfNode(short nodeID) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);

        if (nodeIndex == -1) {
            throw new NodeIDNotExistException("The given nodeID, %d, does not exists in the node pool!");
        }
        return this.numberOfVerticesOfNodes[nodeIndex];
    }

    public void changeStartVertexIDOfNode(short nodeID, long newExternalStartVertex, long newInternalStartVertex) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException(String.format("The given nodeID, %d, does not exists in the node pool!", nodeID));
        }
        this.startExternalVertexIDs[nodeIndex] = newExternalStartVertex;
        this.startInternalVertexIDs[nodeIndex] = newInternalStartVertex;
    }

    public void changeEndVertexIDOfNode(short nodeID, long newExternalEndVertex, long newInternalEndVertex) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException(String.format("The given nodeID, %d, does not exists in the node pool!", nodeID));
        }
        this.endExternalVertexIDs[nodeIndex] = newExternalEndVertex;
        this.endInternalVertexIDs[nodeIndex] = newInternalEndVertex;
    }

    public long getStartExternalVertexIDOfNode(short nodeID) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException(String.format("The given nodeID, %d, does not exists in the node pool!", nodeID));
        }
        return this.startExternalVertexIDs[nodeIndex];
    }

    public long getEndExternalVertexIDOfNode(short nodeID) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException(String.format("The given nodeID, %d, does not exists in the node pool!", nodeID));
        }
        return this.endExternalVertexIDs[nodeIndex];
    }

    public long getStartInternalVertexIDOfNode(short nodeID) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException(String.format("The given nodeID, %d, does not exists in the node pool!", nodeID));
        }
        return this.startInternalVertexIDs[nodeIndex];
    }

    public long getEndInternalVertexIDOfNode(short nodeID) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException(String.format("The given nodeID, %d, does not exists in the node pool!", nodeID));
        }
        return this.endInternalVertexIDs[nodeIndex];
    }

    private int getIndexOfNode(short nodeID) {
        for (int i = 0; i < this.nodesIDs.length; i++) {
            if (nodeID == this.nodesIDs[i]) return i;
        }
        return -1;
    }

    public short getNodeID(int index) {
        return this.nodesIDs[index];
    }


    public int getStartLineNumberOfVerticesFile(short nodeID) throws NodeIDNotExistException {
        int indexOfNode = this.getIndexOfNode(nodeID);
        int startLine = 1;
        for (int i = 0; i < indexOfNode; i++) {
            startLine += this.getNumOfVerticesOfNode(this.getNodeID(i));
        }
        return startLine;
    }

    public int getEndLineNumberOfVerticesFile(short nodeID) throws NodeIDNotExistException {
        int indexOfNode = this.getIndexOfNode(nodeID);
        int endLine = 0;
        for (int i = 0; i <= indexOfNode; i++) {
            endLine += this.getNumOfVerticesOfNode(this.getNodeID(i));
        }
        return endLine;
    }


    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(this.numOfVertices);
        p_exporter.writeInt(this.numOfEdges);
        p_exporter.writeInt(this.isDirected);
        p_exporter.writeShortArray(this.nodesIDs);

        p_exporter.writeLongArray(this.startExternalVertexIDs);
        p_exporter.writeLongArray(this.startInternalVertexIDs);

        p_exporter.writeLongArray(this.endExternalVertexIDs);
        p_exporter.writeLongArray(this.endInternalVertexIDs);

        p_exporter.writeIntArray(this.numberOfVerticesOfNodes);
    }

    @Override
    public void importObject(Importer p_importer) {
        this.numOfVertices = p_importer.readInt(this.numOfVertices);
        this.numOfEdges = p_importer.readInt(this.numOfEdges);
        this.isDirected = p_importer.readInt(this.isDirected);
        this.nodesIDs = p_importer.readShortArray(this.nodesIDs);

        this.startExternalVertexIDs = p_importer.readLongArray(this.startExternalVertexIDs);
        this.startInternalVertexIDs = p_importer.readLongArray(this.startInternalVertexIDs);

        this.endExternalVertexIDs = p_importer.readLongArray(this.endExternalVertexIDs);
        this.endInternalVertexIDs = p_importer.readLongArray(this.endInternalVertexIDs);

        this.numberOfVerticesOfNodes = p_importer.readIntArray(this.numberOfVerticesOfNodes);
    }

    @Override
    public int sizeofObject() {
        return 3 * Integer.BYTES + ObjectSizeUtil.sizeofShortArray(this.nodesIDs) + ObjectSizeUtil.sizeofLongArray(this.startExternalVertexIDs)
                + ObjectSizeUtil.sizeofLongArray(this.startInternalVertexIDs) + ObjectSizeUtil.sizeofLongArray(this.endInternalVertexIDs) +
                ObjectSizeUtil.sizeofLongArray(this.endExternalVertexIDs) + ObjectSizeUtil.sizeofIntArray(this.numberOfVerticesOfNodes);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.nodesIDs.length; i++) {
            short nodeID = this.nodesIDs[i];
            long numberOfVertices = this.numberOfVerticesOfNodes[i];

            long startExternalVertexID = this.startExternalVertexIDs[i];
            long startInternalVertexID = this.startInternalVertexIDs[i];

            long endExternalVertexID = this.endExternalVertexIDs[i];
            long endInternalVertexID = this.startInternalVertexIDs[i];

            sb.append(String.format("Nodemetadata for node %d\nNumber of Vertices: %d\nStartinternalvertix: %d\nStartexternalvertix: %d\nEndinternalvertix: %d\nEndexternalvertix: %d\n\n",
                    nodeID, numberOfVertices, startInternalVertexID, startExternalVertexID, endInternalVertexID, endExternalVertexID));
        }

        return String.format("Number of vertices: %d\nNumber of Edges: %d\nIs directed: " + this.isDirected + "\n" + sb.toString(), this.numOfVertices, this.numOfEdges);
    }
}
