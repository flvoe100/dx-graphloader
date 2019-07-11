package de.hhu.bsinfo.dxgraphloader.edgeLoader;

import de.hhu.bsinfo.dxgraphloader.edgeLoader.model.Edge;
import de.hhu.bsinfo.dxgraphloader.edgeLoader.model.EdgeLoader;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.NodeIDNotExistException;
import de.hhu.bsinfo.dxgraphloader.util.Util;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.Vertex;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class LDBCEdgeLoader extends EdgeLoader {
    private final Logger LOGGER = LogManager.getFormatterLogger(LDBCEdgeLoader.class);

    private ChunkLocalService localService;
    private ChunkService chunkService;

    private HashMap<Long, List<Long>> neighbors = new HashMap<>();

    private short currentNodeID;

    public LDBCEdgeLoader(short currentNodeID, ChunkLocalService localService, ChunkService chunkService) {
        this.localService = localService;
        this.chunkService = chunkService;

        this.currentNodeID = currentNodeID;
    }

    @Override
    public void loadEdges(String filePath,  HashMap<Long, Integer> idMapper, Vertex[] vertices, LoadingMetaData metaData, boolean saveVertex) {
        this.idMapper = idMapper;
        this.metaData = metaData;
        this.setVertices(vertices);
        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new BufferedInputStream(
                                Files.newInputStream(Paths.get(filePath), StandardOpenOption.READ),
                                1000000),
                        StandardCharsets.US_ASCII))) {
            String line = null;
            Edge e = new Edge();
            Vertex v = new Vertex();

            while ((line = br.readLine()) != null) {
                e.setID(ChunkID.INVALID_ID);
                v.setID(ChunkID.INVALID_ID);
                String[] tmp = line.split("\\s");
                long left = Long.parseLong(tmp[0], 10);
                if (Util.isInRange(this.metaData.getStartExternalVertexIDOfNode(currentNodeID), this.metaData.getEndExternalVertexIDOfNode(currentNodeID), left)) {
                    long right = Long.parseLong(tmp[1], 10);
                    e.setDestVertexID(idMapper.get(left));
                    if (Util.isInRange(this.metaData.getStartExternalVertexIDOfNode(currentNodeID), this.metaData.getEndExternalVertexIDOfNode(currentNodeID), right)) {
                        e.setSourceVertexID(idMapper.get(right));
                    } else {
                        long extRight = this.metaData.getInternalVertexIDOfExternalID(right);
                        e.setSourceVertexID(extRight);
                    }
                    chunkService.put().put(e);
                    localService.createLocal().create(e);

                    if (saveVertex) {
                        addNeighbourOfVertex(e.getSourceVertexID(), e.getDestVertexID());
                        if(!metaData.isDirected()) {
                            addNeighbourOfVertex(e.getDestVertexID(), e.getSourceVertexID());
                        }
                    } else {
                        addNeighbourOfVertex(e.getSourceVertexID(), e.getID());
                        if(!metaData.isDirected()) {
                            addNeighbourOfVertex(e.getDestVertexID(), e.getID());
                        }
                    }
                }
                continue;

            }
            updateVertices();


        } catch (IOException | NodeIDNotExistException e) {
            e.printStackTrace();
        }
    }

    private void addNeighbourOfVertex(final long p_id, final long p_neighbourID) {
        LOGGER.info("Updating vertices neighbour lists...");
        if (neighbors.containsKey(p_id)) {
            neighbors.get(p_id).add(p_id);
        } else {
            ArrayList<Long> newNeighbourList = new ArrayList<>();
            newNeighbourList.add(p_neighbourID);
            neighbors.put(p_id, newNeighbourList);

        }
        LOGGER.info("Finished updating vertices neighbour lists...");
    }

    private void updateVertices() {
        for (Vertex v : vertices) {
            ArrayList<Long> p_neighbours = (ArrayList<Long>) neighbors.get(v.getID());
            v.setNeighborIDs(p_neighbours.stream().mapToLong(l -> l).toArray());
            chunkService.put().put(v);
        }
    }
}
