package de.hhu.bsinfo.dxgraphloader;


import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.PropertiesLoader;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.WrongGraphInputException;
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

public class GraphLoader {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoader.class);

    private NameserviceService nameService;
    private SynchronizationService syncService;
    private ChunkService chunkService;
    private ChunkLocalService chunkLocalService;

    private PropertiesLoader propertiesLoader;
    private VertexLoader vertexLoader;
    private List<Short> nodes;
    private short ownID;

    private LoadingMetaData metaData;

    private final String PROPERTIES_BARRIER = "PBAR";
    private final String VERTICES_BARRIER = "VBAR";
    private final String GRAPH_METADATA = "GMD";
    private final int TIMEOUT = 1000;


    public GraphLoader(List<Short> nodes, short currentId, PropertiesLoader propertiesLoader, VertexLoader vertexLoader,
                       NameserviceService nameService, SynchronizationService syncService, ChunkService chunkService, ChunkLocalService chunkLocalService) {
        this.nameService = nameService;
        this.syncService = syncService;
        this.chunkService = chunkService;
        this.chunkLocalService = chunkLocalService;

        this.nodes = nodes;
        this.propertiesLoader = propertiesLoader;
        this.vertexLoader = vertexLoader;
        this.ownID = currentId;
    }

    public void loadGraph(String propertiesFilePath, String verticesFilePath, String datasetPrefix) {
        LOGGER.info("%d: Start loading Graph", this.ownID);
        this.loadProperties(propertiesFilePath, datasetPrefix);
        this.loadVertices(verticesFilePath);

    }

    public LoadingMetaData getLoadingMetaData() {
        return this.metaData;
    }

    public int getNumberOfLoadedVertices() {
        return this.vertexLoader.idMapper.size();
    }

    private void loadProperties(String propertiesFilePath, String datasetPrefix) {
        LOGGER.info("Node %d: Start loading Graphproperties", this.ownID);

        if (this.nodes.get(0) == this.ownID) {
            loadPropertiesByCoordinator(propertiesFilePath, datasetPrefix);
        } else {
            loadPropertiesBySlaveNodes();
        }
    }

    private void loadPropertiesByCoordinator(String propertiesFilePath, String datasetPrefix) {
        LOGGER.info("Coordinator %d: loading Properties", this.ownID);

        int barrierID = this.syncService.barrierAllocate(this.nodes.size());
        this.nameService.register(barrierID, PROPERTIES_BARRIER);
        LOGGER.info("Coordinator %d: Properties Barrier created", this.ownID);

        try {
            this.metaData = this.propertiesLoader.loadProperties(propertiesFilePath, datasetPrefix, this.nodes);
            this.chunkLocalService.createLocal().create(this.metaData);
            this.chunkService.put().put(this.metaData);
        } catch (WrongGraphInputException e) {
            e.printStackTrace();
        }

        this.nameService.register(metaData.getID(), this.GRAPH_METADATA);
        LOGGER.info("Coordinator %d: Saved and registered metadata", this.ownID);

        this.syncService.barrierSignOn(barrierID, 0, true);
        this.syncService.barrierFree(barrierID);
        LOGGER.info("Coordinator %d: Properties barrier freed", this.ownID);

    }

    private void loadPropertiesBySlaveNodes() {

        int barrierID = BarrierID.INVALID_ID;
        long metaDataID = ChunkID.INVALID_ID;
        LOGGER.info("Slave %d: Load Metadata from coordinator", this.ownID);
        while (metaDataID == ChunkID.INVALID_ID) {
            metaDataID = this.nameService.getChunkID(this.GRAPH_METADATA, this.TIMEOUT);
        }
        this.metaData = new LoadingMetaData(this.nodes.size());
        this.metaData.setID(metaDataID);
        this.chunkService.get().get(this.metaData);
        LOGGER.info("Slave %d: Loaded Metadata from coordinator", this.ownID);

        LOGGER.info("Slave %d: Entering properties barrier", this.ownID);

        while (barrierID == BarrierID.INVALID_ID) {
            barrierID = (int) this.nameService.getChunkID(this.PROPERTIES_BARRIER, this.TIMEOUT);
        }
        this.syncService.barrierSignOn(barrierID, 0, true);
    }

    private void loadVertices(String verticesFilePath) {
        LOGGER.info("Node %d: Start loading vertices", this.ownID);
        if(this.ownID == this.nodes.get(0)){
            this.coordinatorLoadsVertices(verticesFilePath);
        } else {
            this.slavesLoadVertices(verticesFilePath);
        }


    }

    private void slavesLoadVertices(String verticesFilePath) {
        LOGGER.info("Slave %d: Load vertices from file", this.ownID);
        this.metaData = this.vertexLoader.loadVertices(verticesFilePath, this.metaData);
        LOGGER.info("Slave %d: Finished loading vertices from file", this.ownID);

        int barrierID = BarrierID.INVALID_ID;
        LOGGER.info("Slave %d: Wait for others...(Vertices barrier)", this.ownID);
        while (barrierID == BarrierID.INVALID_ID) {
            barrierID = (int) this.nameService.getChunkID(this.VERTICES_BARRIER, this.TIMEOUT);
        }
        this.syncService.barrierSignOn(barrierID, 0, true);
        LOGGER.info("Slave %d: Load global meta data", this.ownID);
        if(!this.chunkService.get().get(this.metaData)){
            LOGGER.error("Slave %d: Could not load new global metadata", this.ownID);
        }
        LOGGER.info("GET");
        LOGGER.info(this.metaData.toString());
        LOGGER.info("Slave %d: Loaded new global meta data", this.ownID);
    }

    private void coordinatorLoadsVertices(String verticesFilePath) {
        LOGGER.info("Coordinator %d: Create vertices barrier", this.ownID);
        int barrierID = this.syncService.barrierAllocate(this.nodes.size());
        this.nameService.register(barrierID, this.VERTICES_BARRIER);
        LOGGER.info("Coordinator %d: Load vertices from file", this.ownID);
        this.metaData =  this.vertexLoader.loadVertices(verticesFilePath, this.metaData);
        LOGGER.info("Coordinator %d: Finished loading vertices from file", this.ownID);
        this.syncService.barrierSignOn(barrierID, 0, true);
        this.syncService.barrierFree(barrierID);
    }

    private void loadEdges(String edgesFilePath) {

    }
}
