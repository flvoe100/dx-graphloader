package de.hhu.bsinfo.dxgraphloader.vertexLoader;

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

public class LDBCVerticesLoader extends VertexLoader {

    private ChunkLocalService chunkService;


    public LDBCVerticesLoader() {
        super();
    }

    public LDBCVerticesLoader(ChunkLocalService service) {
        this.chunkService = service;
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
            long cntVertices = 0;
            while ((line = br.readLine()) != null) {
                long vid = Long.parseLong(line.split("\\s")[0]);

                if (!this.idMapper.containsKey(vid)) {
                    Vertex v = new Vertex(vid);
                    this.chunkService.createLocal().create(v);
                    this.idMapper.put(vid, v.getID());
                }

                cntVertices++;
                if (cntVertices % outMod == 0) {
                    System.out.println(String.format("Processing: %dM vertices finished...", (cntVertices / outMod)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
