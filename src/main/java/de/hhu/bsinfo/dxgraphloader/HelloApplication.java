package de.hhu.bsinfo.dxgraphloader;

import de.hhu.bsinfo.dxgraphloader.vertexLoader.LDBCVerticesLoader;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.VertexLoader;
import de.hhu.bsinfo.dxram.app.Application;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.engine.DXRAMVersion;
import de.hhu.bsinfo.dxram.generated.BuildConfig;


/**
 * "Hello world" example DXRAM application.
 *
 * @author Stefan Nothaas, stefan.nothaas@hhu.de, 17.05.17
 */
public class HelloApplication extends Application {
    @Override
    public DXRAMVersion getBuiltAgainstVersion() {
        return BuildConfig.DXRAM_VERSION;
    }

    @Override
    public String getApplicationName() {
        return "HelloApplication";
    }

    @Override
    public void main(final String[] p_args) {


        ChunkLocalService chunkLocalService = getService(ChunkLocalService.class);
        ChunkService chunkService = getService(ChunkService.class);
        VertexLoader vertexLoader = new LDBCVerticesLoader(chunkLocalService, chunkService, 555270053);
        long mem = Runtime.getRuntime().freeMemory();
        vertexLoader.loadVertices("/home/voelz/projektarbeit/datagen-9_3-zf.v");
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
