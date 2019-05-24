package de.hhu.bsinfo.dxgraphloader;

import de.hhu.bsinfo.dxgraphloader.metaDataLoader.LDBCPropertiesLoader;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.LDBCDistVerticesLoader;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.LDBCLocalVerticesLoader;
import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;

import java.util.List;


/**
 * "Hello world" example DXRAM application.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class GraphloaderApplication extends Application {
    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "GraphloaderApplication";
    }

    @Override
    public void main(final String[] p_args) {

        ChunkLocalService chunkLocalService = this.getService(ChunkLocalService.class);
        ChunkService chunkService = this.getService(ChunkService.class);
        BootService bootService = this.getService(BootService.class);
        NameserviceService nameService = this.getService(NameserviceService.class);
        SynchronizationService syncService = this.getService(SynchronizationService.class);
        NetworkService networkService = this.getService(NetworkService.class);

        String propFilePath = p_args[0];
        String verticeFilePath = p_args[1];
        String datasetPrefix = p_args[2];

        short currentNodeID = bootService.getNodeID();
        short coordinatorID = bootService.getOnlinePeerNodeIDs().get(0);
        boolean isCoordinator = coordinatorID == currentNodeID;
        LDBCDistVerticesLoader verticesLoader = new LDBCDistVerticesLoader(currentNodeID, isCoordinator,coordinatorID, chunkLocalService, chunkService, nameService, networkService, syncService);
        long mem = Runtime.getRuntime().freeMemory();
        GraphLoader graphLoader = new GraphLoader(bootService.getOnlinePeerNodeIDs(), bootService.getNodeID(), new LDBCPropertiesLoader(bootService.getOnlinePeerNodeIDs()),
                verticesLoader, nameService, syncService, chunkService, chunkLocalService);

        graphLoader.loadGraph(propFilePath, verticeFilePath, datasetPrefix);
        LoadingMetaData metaData = graphLoader.getLoadingMetaData();


        System.out.println(metaData.toString());
        


        // Put your application code running on the DXRAM node/peer here
    }

    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }
}
