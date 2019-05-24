package de.hhu.bsinfo.dxgraphloader.metadataLoader.model;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

public class LoadingMetaData extends AbstractChunk {


    private int numOfVertices = 0;
    private int numOfEdges = 0;
    private int isDirected = 0;

    private short[] nodesIDs;
    private long[] startVertexIDs;
    private long[] endVertexIDs;
    private int[] numberOfVerticesOfNodes;

    public LoadingMetaData(int numberOfNodes) {
        this.nodesIDs = new short[numberOfNodes];
        this.startVertexIDs = new long[numberOfNodes];
        this.endVertexIDs = new long[numberOfNodes];
        this.numberOfVerticesOfNodes = new int[numberOfNodes];
        Arrays.fill(this.nodesIDs, (short) 0);
        Arrays.fill(this.startVertexIDs, 0);
        Arrays.fill(this.endVertexIDs, 0);
        Arrays.fill(this.numberOfVerticesOfNodes, 0);

    }

    public LoadingMetaData(short[] nodeIDs) {
        this.nodesIDs = nodeIDs;
        this.startVertexIDs = new long[nodeIDs.length];
        this.endVertexIDs = new long[nodeIDs.length];
        this.numberOfVerticesOfNodes = new int[nodeIDs.length];
        Arrays.fill(this.startVertexIDs, 0);
        Arrays.fill(this.endVertexIDs, 0);
        Arrays.fill(this.numberOfVerticesOfNodes, 0);
    }

    public LoadingMetaData(int numOfVertices, int numOfEdges, int isDirected) {
        this.numOfVertices = numOfVertices;
        this.numOfEdges = numOfEdges;
        this.isDirected = isDirected;
    }

    public LoadingMetaData(int numOfVertices, int numOfEdges, short... nodeIDs) {
        this.nodesIDs = nodeIDs;
        this.startVertexIDs = new long[nodeIDs.length];
        this.endVertexIDs = new long[nodeIDs.length];
        this.numberOfVerticesOfNodes = new int[nodeIDs.length];
        this.numOfVertices = numOfVertices;
        this.numOfEdges = numOfEdges;
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
        this.startVertexIDs[index] = startVertexID;
        this.endVertexIDs[index] = endVertexID;
        this.numberOfVerticesOfNodes[index] = partitionSize;
    }

    public int getNumOfVerticesOfNode(short nodeID) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException("The given nodeID, %d, does not exists in the node pool!");
        }
        return this.numberOfVerticesOfNodes[nodeIndex];
    }

    public void changeStartVertexIDOfNode(short nodeID, long newStartVertex) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException(String.format("The given nodeID, %d, does not exists in the node pool!", nodeID));
        }
        this.startVertexIDs[nodeIndex] = newStartVertex;
    }

    public void changeEndVertexIDOfNode(short nodeID, long newEndVertex) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException(String.format("The given nodeID, %d, does not exists in the node pool!", nodeID));
        }
        this.endVertexIDs[nodeIndex] = newEndVertex;
    }

    public long getStartVertexIDOfNode(short nodeID) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException(String.format("The given nodeID, %d, does not exists in the node pool!", nodeID));
        }
        return this.startVertexIDs[nodeIndex];
    }

    public long getEndVertexIDOfNode(short nodeID) throws NodeIDNotExistException {
        int nodeIndex = this.getIndexOfNode(nodeID);
        if (nodeID == -1) {
            throw new NodeIDNotExistException(String.format("The given nodeID, %d, does not exists in the node pool!", nodeID));
        }
        return this.endVertexIDs[nodeIndex];
    }

    private int getIndexOfNode(short nodeID) {
        for (int i = 0; i < this.nodesIDs.length; i++) {
            if (nodeID == this.nodesIDs[i]) return i;
        }
        return -1;
    }

    private short getNodeID(int index) {
        return this.nodesIDs[index];
    }


    public int getStartLineNumberOfVerticesFile(short nodeID) throws NodeIDNotExistException {
        int indexOfNode = this.getIndexOfNode(nodeID);
        int startLine = 0;
        for (int i = 0; i < indexOfNode; i++) {
            startLine += this.getNumOfVerticesOfNode(this.getNodeID(i));
        }
        return startLine;
    }

    public int getEndLineNumberOfVerticesFile(short nodeID) throws NodeIDNotExistException {
        int indexOfNode = this.getIndexOfNode(nodeID);

        return indexOfNode * this.getNumOfVerticesOfNode(nodeID);
    }


    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeInt(this.numOfVertices);
        p_exporter.writeInt(this.numOfEdges);
        p_exporter.writeInt(this.isDirected);
        p_exporter.writeShortArray(this.nodesIDs);
        p_exporter.writeLongArray(this.startVertexIDs);
        p_exporter.writeLongArray(this.endVertexIDs);
        p_exporter.writeIntArray(this.numberOfVerticesOfNodes);
    }

    @Override
    public void importObject(Importer p_importer) {
        this.numOfVertices = p_importer.readInt(this.numOfVertices);
        this.numOfEdges = p_importer.readInt(this.numOfEdges);
        this.isDirected = p_importer.readInt(this.isDirected);
        this.nodesIDs = p_importer.readShortArray(this.nodesIDs);
        this.startVertexIDs = p_importer.readLongArray(this.startVertexIDs);
        this.endVertexIDs = p_importer.readLongArray(this.endVertexIDs);
        this.numberOfVerticesOfNodes = p_importer.readIntArray(this.numberOfVerticesOfNodes);
    }

    @Override
    public int sizeofObject() {
        return (3 * Integer.BYTES) + ObjectSizeUtil.sizeofShortArray(this.nodesIDs) + ObjectSizeUtil.sizeofLongArray(this.startVertexIDs) +
                ObjectSizeUtil.sizeofLongArray(this.endVertexIDs) + ObjectSizeUtil.sizeofIntArray(this.numberOfVerticesOfNodes);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < this.nodesIDs.length; i++) {
            short nodeID = this.nodesIDs[i];
            long numberOfVertices = this.numberOfVerticesOfNodes[i];
            long startVertex = this.startVertexIDs[i];
            long endVertex = this.endVertexIDs[i];
            sb.append(String.format("Nodemetadata for node %d\nNumber of Vertices: %d\nStartvertix: %d\nEndvertix: %d\n", nodeID, numberOfVertices, startVertex, endVertex));
        }

        return String.format("Number of vertices: %d\nNumber of Edges: %d\nIs directed: " + this.isDirected + "\n" + sb.toString(), this.numOfVertices, this.numOfEdges);
    }
}
