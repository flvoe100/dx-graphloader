package de.hhu.bsinfo.dxgraphloader;

import de.hhu.bsinfo.dxgraphloader.metadataLoader.LDBCPropertiesLoader;
import de.hhu.bsinfo.dxgraphloader.metadataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.LDBCDistVerticesLoader;
import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;


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

        String datasetPrefix = p_args[0];
        String propertiesPath = p_args[1];
        String verticePath = p_args[3];

        ChunkLocalService chunkLocalService = this.getService(ChunkLocalService.class);
        ChunkService chunkService = this.getService(ChunkService.class);
        BootService bootService = this.getService(BootService.class);
        NameserviceService nameService = this.getService(NameserviceService.class);
        SynchronizationService syncService = this.getService(SynchronizationService.class);


        short currentNodeID = bootService.getNodeID();
        LDBCDistVerticesLoader verticesLoader = new LDBCDistVerticesLoader(currentNodeID, chunkLocalService);
        long mem = Runtime.getRuntime().freeMemory();
        GraphLoader graphLoader = new GraphLoader(bootService.getOnlinePeerNodeIDs(), bootService.getNodeID(), new LDBCPropertiesLoader(bootService.getOnlinePeerNodeIDs()),
                verticesLoader, nameService, syncService, chunkService, chunkLocalService);

        graphLoader.loadGraph(propertiesPath, verticePath, datasetPrefix);
        LoadingMetaData metaData = graphLoader.getLoadingMetaData();


        System.out.println("Memoryusage: " + (Runtime.getRuntime().freeMemory() - mem) / 1e+6);
        System.out.println("-------------");
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
