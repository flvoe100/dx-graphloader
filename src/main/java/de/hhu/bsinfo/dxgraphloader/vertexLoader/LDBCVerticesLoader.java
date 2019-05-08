package de.hhu.bsinfo.dxgraphloader.vertexLoader;

import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.Vertex;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.VertexLoader;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

public class LDBCVerticesLoader extends VertexLoader {

    private ChunkLocalService localService;
    private ChunkService chunkService;
    private int NUM_OF_VERTICES;


    public LDBCVerticesLoader() {
        super();
    }

    public LDBCVerticesLoader(ChunkLocalService localService, ChunkService chunkService, int verticesCount) {
        this.localService = localService;
        this.chunkService = chunkService;
        this.NUM_OF_VERTICES = verticesCount;
    }


    @Override
    public void loadVertices(String filePath) {
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

            long[] ids = localService.reserveLocal().reserve(this.NUM_OF_VERTICES);
            int[] sizesOfVertices = new int[this.NUM_OF_VERTICES];
            Arrays.fill(sizesOfVertices, v.sizeofObject());
            int created = localService.createReservedLocal().create(ids, this.NUM_OF_VERTICES, sizesOfVertices);

            if(created != this.NUM_OF_VERTICES) {
                System.out.println("Create reserve local did not succeed");
                return;
            }


            while ((line = br.readLine()) != null) {
                long vid = Long.parseLong(line.split("\\s")[0]);

                v.setID(ids[cntVertices++]);
                v.setExternalId(vid);
                this.idMapper.put(vid, v.getID());

                if (cntVertices % outMod == 0) {
                    System.out.println(String.format("Processing: %dM vertices finished...", (cntVertices / outMod)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
