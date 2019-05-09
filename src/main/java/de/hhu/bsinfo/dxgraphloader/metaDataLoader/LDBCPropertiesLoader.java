package de.hhu.bsinfo.dxgraphloader.metaDataLoader;


import com.jramoyo.io.IndexedFileReader;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.PropertiesLoader;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.WrongGraphInputException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.SortedMap;

public class LDBCPropertiesLoader implements PropertiesLoader  {

    private final String PREFIX_NUM_OF_VERTICES = ".meta.vertices = ";
    private final String PREFIX_NUM_OF_EDGES = ".meta.edges = ";
    private final String PREFIX_IS_DIRECTED = ".meta.vertices = ";
    private String prefixDataset;
    private String datasetPath;
    private LoadingMetaData metaData;
    private List<Short> nodes;


    public LDBCPropertiesLoader(String datasetPrefix, String datasetPath) {
        this.prefixDataset = datasetPrefix;
        this.datasetPath = datasetPath;
    }


    @Override
    public LoadingMetaData loadProperties(String propertiesPath, List<Short> nodes) throws WrongGraphInputException {
        this.metaData = new LoadingMetaData();
        readPropertiesFile(propertiesPath);
        determineParitionsSizes();
        return metaData;
    }

    private void readPropertiesFile(String filePath) {
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

                if (line.contains(PREFIX_NUM_OF_VERTICES)) {
                    numOfVertices = Integer.parseInt(line.split(this.prefixDataset + PREFIX_NUM_OF_VERTICES)[1]);
                }

                if (line.contains(PREFIX_NUM_OF_EDGES)) {
                    numOfEdges = Integer.parseInt(line.split(this.prefixDataset + PREFIX_NUM_OF_EDGES)[1]);
                }

                if (line.contains(PREFIX_IS_DIRECTED)) {
                    isDirected = Boolean.parseBoolean(line.split(this.prefixDataset + PREFIX_IS_DIRECTED)[1]);
                }
            }
            this.metaData.setNumOfVertices(numOfVertices);
            this.metaData.setNumOfEdges(numOfEdges);
            this.metaData.setDirected(isDirected);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void determineParitionsSizes() throws WrongGraphInputException {
        if(this.nodes.size() > this.metaData.getNumOfVertices()) {
            throw new WrongGraphInputException("ERROR: Too small number of vertices for the given number of datanodes!");
        }
        int startReadIndex = 0;
        int totalNumberOfVertices = this.metaData.getNumOfVertices();
        for (int i = 0; i < this.nodes.size(); i++) {
            short nodeID = this.nodes.get(i);
            long startVertexID = 0, endVertexID = 0;
            //maybe better partitioning?
            //slice into even partitions
            int sizeOfPartition = i != this.nodes.size() - 1 ? totalNumberOfVertices * 1 / this.nodes.size() : totalNumberOfVertices * 1 / this.nodes.size() + totalNumberOfVertices % this.nodes.size();
            SortedMap<Integer, String> partitionLines;
            try(final IndexedFileReader indexedFileReader = new IndexedFileReader(new File(this.datasetPath), StandardCharsets.US_ASCII)) {
                partitionLines = indexedFileReader.readLines(startReadIndex, startReadIndex+sizeOfPartition);
                startVertexID = Integer.parseInt(partitionLines.get(partitionLines.firstKey()));
                endVertexID = Integer.parseInt(partitionLines.get(partitionLines.lastKey()));
                startReadIndex += sizeOfPartition;
            } catch(IOException e) {
                e.printStackTrace();
            };

            this.metaData.addNodeMetaData(i, nodeID, startVertexID, endVertexID, sizeOfPartition);
        }
    }

}
