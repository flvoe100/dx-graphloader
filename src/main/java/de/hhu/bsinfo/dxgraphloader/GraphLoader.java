package de.hhu.bsinfo.dxgraphloader;


import de.hhu.bsinfo.dxgraphloader.metadataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metadataLoader.model.NodeIDNotExistException;
import de.hhu.bsinfo.dxgraphloader.metadataLoader.model.PropertiesLoader;
import de.hhu.bsinfo.dxgraphloader.model.WrongGraphInputException;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.VertexLoader;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;

import java.util.List;

public class GraphLoader {

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
        System.out.println("Start loading Graph");
        try {
            this.loadProperties(propertiesFilePath, datasetPrefix);
        } catch (NodeIDNotExistException e) {
            e.printStackTrace();
        }
        this.loadVertices(verticesFilePath);

    }

    public LoadingMetaData getLoadingMetaData() {
        return this.metaData;
    }

    public int getNumberOfLoadedVertices() {
        return this.vertexLoader.idMapper.size();
    }

    private void loadProperties(String propertiesFilePath, String datasetPrefix) throws NodeIDNotExistException {
        System.out.println("Start loading Graphproperties");
        if (this.nodes.get(0) == this.ownID) {
            loadPropertiesByCoordinator(propertiesFilePath, datasetPrefix);
        } else {
            loadPropertiesBySlaveNodes();
        }
    }

    private void loadPropertiesByCoordinator(String propertiesFilePath, String datasetPrefix) {
        int barrierID = this.syncService.barrierAllocate(this.nodes.size());
        this.nameService.register(barrierID, PROPERTIES_BARRIER);

        try {
            this.metaData = this.propertiesLoader.loadProperties(propertiesFilePath, datasetPrefix);
            this.chunkLocalService.createLocal().create(this.metaData);
        } catch (WrongGraphInputException e) {
            e.printStackTrace();
        }
        System.out.println("Loading done, now register");
        this.nameService.register(metaData, this.GRAPH_METADATA);
        System.out.println("Register done");
        this.syncService.barrierSignOn(barrierID, 0, true); //Ask if this works?!?
        System.out.println("Free barrier");
        this.syncService.barrierFree(barrierID);
    }

    private void loadPropertiesBySlaveNodes() {
        System.out.println("Wait for finishing loading properties to load");
        int barrierID = BarrierID.INVALID_ID;
        long metaDataID = ChunkID.INVALID_ID;

        while (metaDataID == ChunkID.INVALID_ID) {
            metaDataID = this.nameService.getChunkID(this.GRAPH_METADATA, this.TIMEOUT);
        }
        this.metaData = new LoadingMetaData(2);
        this.metaData.setID(metaDataID);
        boolean happened = this.chunkService.get().get(this.metaData);
        if (happened) System.err.println("Could not load meta data!!!!!!!!!!!!!!");
        System.out.println("Loading registered metadata done");
        while (barrierID == BarrierID.INVALID_ID) {
            barrierID = (int) this.nameService.getChunkID(this.PROPERTIES_BARRIER, this.TIMEOUT);
        }
        this.syncService.barrierSignOn(barrierID, 0, true);
    }

    private void loadVertices(String verticesFilePath) {
        System.out.println("Start loading vertices on" + this.ownID);
        System.out.println("----------- " + this.metaData.getNumOfVertices());
        this.vertexLoader.loadVertices(verticesFilePath, this.metaData);


    }

    private void loadEdges(String edgesFilePath) {

    }
}
