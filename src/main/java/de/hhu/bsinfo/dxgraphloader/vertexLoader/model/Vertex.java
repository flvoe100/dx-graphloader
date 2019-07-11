package de.hhu.bsinfo.dxgraphloader.vertexLoader.model;

import de.hhu.bsinfo.dxmem.data.AbstractChunk;
import de.hhu.bsinfo.dxutils.serialization.Exporter;
import de.hhu.bsinfo.dxutils.serialization.Importer;
import de.hhu.bsinfo.dxutils.serialization.ObjectSizeUtil;

import java.util.Arrays;

public class Vertex extends AbstractChunk {

    private long externalId;
   // private boolean neighborsAreEdgeObjects;
    private long[] neighborIDs = new long[0];

    public Vertex(){}

    public Vertex(long externalId) {
        this.externalId = externalId;
    }



    public long getExternalId() {
        return externalId;
    }

    public void setExternalId(long externalId) {
        this.externalId = externalId;
    }

    /**
     * Check if the neighbor IDs of this vertex refer to actual edge objects
     * that can store data.
     *
     * @return If true, neighbor IDs refer to actual edge objects, false if
     * they refer to the neighbor vertex directly.

    public boolean areNeighborsEdgeObjects() {
        return neighborsAreEdgeObjects;
    }
     */
    /**
     * Add a new neighbour to the currently existing list.
     * This will expand the array by one entry and
     * add the new neighbour at the end.
     *
     * @param p_neighbour
     *         Neighbour vertex Id to add.
     */
    public void addNeighbour(final long p_neighbour) {
        setNeighbourCount(neighborIDs.length + 1);
        neighborIDs[neighborIDs.length - 1] = p_neighbour;
    }

    /**
     * Get the neighbour array.
     *
     * @return Neighbour array with vertex ids.
     */
    public long[] getNeighbours() {
        return neighborIDs;
    }

    /**
     * Get the number of neighbors of this vertex.
     *
     * @return Number of neighbors.
     */
    public int getNeighborCount() {
        return neighborIDs.length;
    }

    /**
     * Resize the neighbour array.
     *
     * @param p_count
     *         Number of neighbours to resize to.
     */
    public void setNeighbourCount(final int p_count) {
        if (p_count != neighborIDs.length) {
            // grow or shrink array
            neighborIDs = Arrays.copyOf(neighborIDs, p_count);
        }
    }

    public void setNeighborIDs(long[] neighborIDs) {
        this.neighborIDs = neighborIDs;
    }

    @Override
    public void exportObject(Exporter p_exporter) {
        p_exporter.writeLong(this.externalId);
       // p_exporter.writeBoolean(this.neighborsAreEdgeObjects);
        p_exporter.writeLongArray(this.neighborIDs);
    }

    @Override
    public void importObject(Importer p_importer) {
        this.externalId = p_importer.readLong(this.externalId);
      //  this.neighborsAreEdgeObjects = p_importer.readBoolean(this.neighborsAreEdgeObjects);
        this.neighborIDs = p_importer.readLongArray(this.neighborIDs);
    }

    @Override
    public int sizeofObject() {
        return Long.BYTES  + ObjectSizeUtil.sizeofLongArray(this.neighborIDs);
    }
}
