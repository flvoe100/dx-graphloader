package de.hhu.bsinfo.dxgraphloader.vertexLoader;

import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.Vertex;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.VertexLoader;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;


import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class LDBCLocalVerticesLoader extends VertexLoader {

    private ChunkLocalService localService;
    private int NUM_OF_VERTICES;


    public LDBCLocalVerticesLoader() {
        super();
    }

    public LDBCLocalVerticesLoader(ChunkLocalService localService, int verticesCount) {
        this.localService = localService;
        this.NUM_OF_VERTICES = verticesCount;
    }


    @Override
    public LoadingMetaData loadVertices(String filePath, LoadingMetaData metaData) {
        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new BufferedInputStream(
                                Files.newInputStream(Paths.get(filePath), StandardOpenOption.READ),
                                1000000),
                        StandardCharsets.US_ASCII))) {
            String line = null;
            final int outMod = 1000000;
            int cntVertices = 0;
            Vertex v = new Vertex();
            System.out.println("Create ID space!");
            long[] ids = new long[this.NUM_OF_VERTICES];
            localService.createLocal().create(ids, this.NUM_OF_VERTICES, v.sizeofObject(), true); //true = aufsteigend
            final long firstId = ids[0];
            long chunkId = firstId;
            ids = null;
            System.gc();
            System.out.println("ID space created!");
            System.out.println("Start processing vertix file!");
            while ((line = br.readLine()) != null) {
                long vid = Long.parseLong(line.split("\\s")[0]);

                v.setID(chunkId++);
                v.setExternalId(vid);

                this.idMapper.put(vid, (int) ((v.getID() - firstId) & 0x0000ffffffffffffL));
                cntVertices++;
                if (cntVertices % outMod == 0) {
                    System.out.println(String.format("Processing: %dM vertices finished...", (cntVertices / outMod)));
                }
            }
            System.out.println("Procesing vertices done!");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return metaData;
    }
}
