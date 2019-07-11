package de.hhu.bsinfo.dxgraphloader;


import de.hhu.bsinfo.dxgraphloader.edgeLoader.model.EdgeLoader;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.PropertiesLoader;
import de.hhu.bsinfo.dxgraphloader.model.KeyValueStoreLoadingException;
import de.hhu.bsinfo.dxgraphloader.model.WrongGraphInputException;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.VertexLoader;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

import static de.hhu.bsinfo.dxgraphloader.util.Util.*;

public class GraphLoader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoader.class);

    private NameserviceService nameService;
    private SynchronizationService syncService;
    private ChunkService chunkService;
    private ChunkLocalService chunkLocalService;

    private PropertiesLoader propertiesLoader;
    private VertexLoader vertexLoader;
    private EdgeLoader edgeLoader;
    private List<Short> nodes;
    private boolean saveVertex;
    private short ownID;

    private LoadingMetaData metaData;

    private final String PROP_BARRIER = "PBARR";
    private final String GMD_BARRIER = "GMDB";
    private final String VERT_BARRIER = "VBARR";
    public static final String GLOBAL_META_DATA = "GMD";


    public GraphLoader(List<Short> nodes, short currentId, boolean saveVertex, PropertiesLoader propertiesLoader, VertexLoader vertexLoader, EdgeLoader edgeLoader,
                       NameserviceService nameService, SynchronizationService syncService, ChunkService chunkService, ChunkLocalService chunkLocalService) {
        this.nameService = nameService;
        this.syncService = syncService;
        this.chunkService = chunkService;
        this.chunkLocalService = chunkLocalService;

        this.nodes = nodes;
        this.propertiesLoader = propertiesLoader;
        this.vertexLoader = vertexLoader;
        this.edgeLoader = edgeLoader;
        this.saveVertex = saveVertex;
        this.ownID = currentId;
    }

    public void loadGraph(String propertiesFilePath, String verticesFilePath, String edgeFilePath, String datasetPrefix) {
        LOGGER.info("%d: Start loading Graph", this.ownID);
        Runtime runtime = Runtime.getRuntime();
        long startTime = System.nanoTime();
        long memoryUsageStart = runtime.totalMemory() - runtime.freeMemory();
        try {
            this.loadProperties(propertiesFilePath, datasetPrefix);
            long memoryUsageProp = runtime.totalMemory() - runtime.freeMemory();
            System.gc();
            long endTimeProp = System.nanoTime();
            long execTimeProp = endTimeProp - startTime;
            long startTimeVertex = System.nanoTime();

            this.loadVertices(verticesFilePath);

            long memoryUsageVertices = runtime.totalMemory() - runtime.freeMemory();
            long endTimeVertex = System.nanoTime();

            long execTimeVert = endTimeVertex - startTimeVertex;
            System.gc();
            long startTimeEdges = System.nanoTime();
            /** this.loadEdges(edgeFilePath);
             long endTimeEdges = System.nanoTime();
             long execTimeEdges = endTimeEdges - startTimeEdges;
             System.gc();
             */
            long memoryUsageEnd = runtime.totalMemory() - runtime.freeMemory();
            long execTimeOverall = execTimeProp + execTimeVert;
            LOGGER.info("Node %d: Properties time: %d nanosecond", this.ownID, execTimeProp);
            LOGGER.info("Node %d: Vertex time: %d nanosecond", this.ownID, execTimeVert);
            //  LOGGER.info("Node %d: Edges time: %d nanosecond", this.ownID, execTimeEdges);
            LOGGER.info("Node %d: Overall time: %d nanosecond", this.ownID, execTimeOverall);
            LOGGER.info("Node %d: Start memory: %d bytes", this.ownID, memoryUsageStart);
            LOGGER.info("Node %d: Properties memory: %d bytes", this.ownID, memoryUsageProp);
            LOGGER.info("Node %d: Vertex memory: %d bytes", this.ownID, memoryUsageVertices);
            LOGGER.info("Node %d: End memory: %d bytes", this.ownID, memoryUsageEnd);
        } catch (WrongGraphInputException e) {
            e.printStackTrace();
        } catch (KeyValueStoreLoadingException e) {
            e.printStackTrace();
        }


    }

    public LoadingMetaData getLoadingMetaData() {
        return this.metaData;
    }

    public int getNumberOfLoadedVertices() {
        return this.vertexLoader.idMapper.size();
    }

    private void loadProperties(String propertiesFilePath, String datasetPrefix) throws WrongGraphInputException, KeyValueStoreLoadingException {
        LOGGER.info("Node %d: Start loading Graphproperties", this.ownID);

        if (isCoordinator()) {
            int barrierID = buildBarrier(this.PROP_BARRIER, this.nodes.size(), this.syncService, this.nameService);
            this.metaData = this.propertiesLoader.loadProperties(propertiesFilePath, datasetPrefix, this.nodes);
            chunkLocalService.createLocal().create(metaData);
            chunkService.put().put(metaData);
            LOGGER.info("Coordinator register global meta data");

            nameService.register(metaData.getID(), GLOBAL_META_DATA);
            LOGGER.info("Register done!");

            waitForAllBarrier(true, barrierID, this.PROP_BARRIER, this.syncService, this.nameService);
            barrierID = buildBarrier(this.GMD_BARRIER, this.nodes.size(), this.syncService, this.nameService);
            waitForAllBarrier(true ,barrierID, GMD_BARRIER, this.syncService, this.nameService);
        } else {
            waitForAllBarrier(false,BarrierID.INVALID_ID, PROP_BARRIER, this.syncService, this.nameService);
            loadGlobalMetaData();
            waitForAllBarrier(false, BarrierID.INVALID_ID, GMD_BARRIER, this.syncService, this.nameService);
        }
    }

    private void loadVertices(String verticesFilePath) throws KeyValueStoreLoadingException {
        LOGGER.info("Node %d: Start loading vertices", this.ownID);
        int barrierID = BarrierID.INVALID_ID;
        if (this.ownID == this.nodes.get(0)) {
            barrierID = buildBarrier(VERT_BARRIER, this.nodes.size(), this.syncService, this.nameService);
        }
        this.vertexLoader.loadVertices(verticesFilePath, metaData);
        if (!isCoordinator()) {
            LOGGER.info(metaData.toString());
            chunkService.get().get(this.metaData);
            LOGGER.info(metaData.toString());

        }
        waitForAllBarrier(isCoordinator(), barrierID, VERT_BARRIER, this.syncService, this.nameService);
    }

    private void loadEdges(String edgesFilePath) {
        this.edgeLoader.loadEdges(edgesFilePath, this.vertexLoader.idMapper, vertexLoader.vertices, metaData, saveVertex);
    }

    private boolean isCoordinator() {
        return this.ownID == this.nodes.get(0);
    }



    private void loadGlobalMetaData() throws KeyValueStoreLoadingException {
        LOGGER.info("Node %d: Loading global meta data");
        long metaDataID = getIdFromNameService(this.GLOBAL_META_DATA, this.nameService);

        this.metaData = new LoadingMetaData();
        this.metaData.setID(metaDataID);
        if (!this.chunkService.get().get(this.metaData)) {
            LOGGER.error("Slave %d: Could not load global meta data");
            throw new KeyValueStoreLoadingException("Could not load global meta data!");
        }
        LOGGER.info("Slave %d: Loaded Metadata from coordinator", this.ownID);
    }

}
