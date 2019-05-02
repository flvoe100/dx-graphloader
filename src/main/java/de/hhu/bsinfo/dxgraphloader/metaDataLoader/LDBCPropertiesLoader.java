package de.hhu.bsinfo.dxgraphloader.metaDataLoader;


import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.PropertiesLoader;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class LDBCPropertiesLoader implements PropertiesLoader {

    private String prefixDataset;
    private final String PREFIX_NUM_OF_VERTICES = ".meta.vertices = ";
    private final String PREFIX_NUM_OF_EDGES = ".meta.edges = ";
    private final String PREFIX_IS_DIRECTED = ".meta.vertices = ";



    public LDBCPropertiesLoader(String datasetPrefix) {
        this.prefixDataset = datasetPrefix;
    }



    @Override
    public LoadingMetaData loadProperties(String filePath) {
        int numOfVertices = 0;
        int numOfEdges = 0;
        boolean isDirected = false;

        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new BufferedInputStream(
                                Files.newInputStream(Paths.get(filePath), StandardOpenOption.READ),
                                1000000),
                        StandardCharsets.US_ASCII))) {
            String line = null;


            while ((line = br.readLine()) != null) {

                if(line.contains(PREFIX_NUM_OF_VERTICES)){
                    numOfVertices = Integer.parseInt(line.split(this.prefixDataset + PREFIX_NUM_OF_VERTICES)[1]);
                }

                if(line.contains(PREFIX_NUM_OF_EDGES)){
                    numOfEdges = Integer.parseInt(line.split(this.prefixDataset + PREFIX_NUM_OF_EDGES)[1]);
                }

                if(line.contains(PREFIX_IS_DIRECTED)) {
                    isDirected = Boolean.parseBoolean(line.split(this.prefixDataset + PREFIX_IS_DIRECTED)[1]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
         LoadingMetaData metaData = new LoadingMetaData(numOfVertices, numOfEdges, isDirected);
        return metaData;
  }

}
