package de.hhu.bsinfo.dxgraphloader;

import de.hhu.bsinfo.dxgraphloader.metaDataLoader.LDBCPropertiesLoader;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.LDBCDistVerticesLoader;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.LDBCLocalVerticesLoader;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.VertexLoader;
import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.boot.BootService;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;

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

        String fileName = p_args[0];
        int numberOfVertices = Integer.parseInt(p_args[1]);
        System.out.println("Input: " + fileName);
        BootService bootService = getService(BootService.class);
        List<Short> nodes = bootService.getOnlinePeerNodeIDs();
        short ownId = bootService.getNodeID();

        ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);
        ChunkService chunkService = getService(ChunkService.class);
        VertexLoader vertexLoader = new LDBCLocalVerticesLoader(chunkLocalService, chunkService,  numberOfVertices );
        long mem = Runtime.getRuntime().freeMemory();
        vertexLoader.loadVertices("/home/voelz/projektarbeit/" + fileName);
        System.out.println(mem - Runtime.getRuntime().freeMemory());
        


        // Put your application code running on the DXRAM node/peer here
    }

    @Override
    public void signalShutdown() {
        // Interrupt any flow of your application and make sure it shuts down.
        // Do not block here or wait for something to shut down. Shutting down of your application
        // must be execute asynchronously
    }
}
