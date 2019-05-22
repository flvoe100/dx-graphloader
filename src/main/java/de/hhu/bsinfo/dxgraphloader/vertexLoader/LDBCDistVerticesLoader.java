package de.hhu.bsinfo.dxgraphloader.vertexLoader;

import de.hhu.bsinfo.dxgraphloader.metadataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metadataLoader.model.NodeIDNotExistException;
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

public class LDBCDistVerticesLoader extends VertexLoader {

    private ChunkLocalService localService;

    private short currentNodeID;


    public LDBCDistVerticesLoader(short currentNodeID, ChunkLocalService chunkLocalService) {
        this.localService = chunkLocalService;
        this.currentNodeID = currentNodeID;
    }


    @Override
    public void loadVertices(String filePath, LoadingMetaData metaData) {
        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new BufferedInputStream(
                                Files.newInputStream(Paths.get(filePath), StandardOpenOption.READ),
                                1000000),
                        StandardCharsets.US_ASCII))) {
            String line = null;
            final int outMod = 1000000;
            int cntVertices = 0;
            int lineNumber = 0;
            int numberOfVerticesOfCurrentNode = metaData.getNumOfVerticesOfNode(this.currentNodeID);
            Vertex v = new Vertex();
            System.out.println(String.format("Create ID space for %d objects!", numberOfVerticesOfCurrentNode));
            long[] ids = new long[numberOfVerticesOfCurrentNode];
            localService.createLocal().create(ids, numberOfVerticesOfCurrentNode, v.sizeofObject(), true); //true = aufsteigend
            final long firstId = ids[0];
            long chunkId = firstId;
            ids = null;
            System.gc();
            System.out.println("ID space created!");
            System.out.println("Start processing vertex file!");
            while ((line = br.readLine()) != null) {
                long vid = Long.parseLong(line.split("\\s")[0]);
                if (!isFirstLineToConsider(lineNumber, metaData)) {
                    lineNumber++;
                    continue;
                }
                if (cntVertices == 0) {
                    System.out.println(String.format("Node %d: Startvertex: %d", this.currentNodeID, vid));
                    metaData.changeStartVertexIDOfNode(this.currentNodeID, vid);
                }
                if (cntVertices == numberOfVerticesOfCurrentNode - 1) {
                    System.out.println(String.format("Node %d: Startvertex: %d", this.currentNodeID, vid));
                    metaData.changeEndVertexIDOfNode(this.currentNodeID, vid);
                }
                v.setID(chunkId++);
                v.setExternalId(vid);

                this.idMapper.put(vid, (int) ((v.getID() - firstId) & 0x0000ffffffffffffL));
                cntVertices++;
                if (cntVertices % outMod == 0) {
                    System.out.println(String.format("Processing: %dM vertices finished...", (cntVertices / outMod)));
                }
            }
            System.out.println("Processing vertices done!");
        } catch (IOException | NodeIDNotExistException e) {
            e.printStackTrace();
        }
    }

    private boolean isFirstLineToConsider(int lineNumber, LoadingMetaData metaData) throws NodeIDNotExistException {
        int firstLineNumber = metaData.getStartLineNumberOfVerticesFile(this.currentNodeID);
        int lastLineNumber = metaData.getEndLineNumberOfVerticesFile(this.currentNodeID);
        return lineNumber >= firstLineNumber && lineNumber <= lastLineNumber;
    }
}
