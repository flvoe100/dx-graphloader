package de.hhu.bsinfo.dxgraphloader.metaDataLoader;


import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.PropertiesLoader;
import de.hhu.bsinfo.dxgraphloader.model.WrongGraphInputException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;


public class LDBCPropertiesLoader implements PropertiesLoader {

    private final Logger LOGGER = LogManager.getFormatterLogger(LDBCPropertiesLoader.class);


    private final String PREFIX_NUM_OF_VERTICES = ".meta.vertices = ";
    private final String PREFIX_NUM_OF_EDGES = ".meta.edges = ";
    private final String PREFIX_IS_DIRECTED = ".meta.vertices = ";
    private List<Short> nodes;


    public LDBCPropertiesLoader(List<Short> nodes) {
        this.nodes = nodes;
    }


    @Override
    public LoadingMetaData
    loadProperties(String propertiesPath, String prefixDataset, List<Short> peers) throws WrongGraphInputException {
        this.nodes = peers;
        short[] nodeIDs = new short[nodes.size()];
        for (int i = 0; i < nodes.size(); i++) {
            nodeIDs[i] = nodes.get(i);
        }
        LoadingMetaData metaData = new LoadingMetaData(nodeIDs);
        LOGGER.info("Coordinator: Start reading properties file");
        readPropertiesFile(propertiesPath, metaData, prefixDataset);
        LOGGER.info("Coordinator: determine Partitions sizes");
        determinePartitionsSizes(metaData);
        return metaData;
    }

    private void readPropertiesFile(String filePath, LoadingMetaData metaData, String prefixDataset) {
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
                    numOfVertices = Integer.parseInt(line.split(prefixDataset + PREFIX_NUM_OF_VERTICES)[1]);
                }

                if (line.contains(PREFIX_NUM_OF_EDGES)) {
                    numOfEdges = Integer.parseInt(line.split(prefixDataset + PREFIX_NUM_OF_EDGES)[1]);
                }

                if (line.contains(PREFIX_IS_DIRECTED)) {
                    isDirected = Boolean.parseBoolean(line.split(prefixDataset + PREFIX_IS_DIRECTED)[1]);
                }
            }
            metaData.setNumOfVertices(numOfVertices);
            metaData.setNumOfEdges(numOfEdges);
            metaData.setDirected(isDirected);
        } catch (IOException e) {
            e.printStackTrace();
        }
        LOGGER.info("Coordinator: Properties file has been read");
    }

    private void determinePartitionsSizes(LoadingMetaData metaData) throws WrongGraphInputException {
        if (this.nodes.size() > metaData.getNumOfVertices()) {
            throw new WrongGraphInputException("ERROR: Too small number of vertices for the given number of datanodes!");
        }
        int totalNumberOfVertices = metaData.getNumOfVertices();
        for (int i = 0; i < this.nodes.size(); i++) {
            short nodeID = this.nodes.get(i);
            long startVertexID = 0, endVertexID = 0;
            //maybe better partitioning?
            //slice into even partitions
            int sizeOfPartition = i != this.nodes.size() - 1 ? totalNumberOfVertices * 1 / this.nodes.size() : totalNumberOfVertices * 1 / this.nodes.size() + totalNumberOfVertices % this.nodes.size();
            metaData.addNodeMetaData(i, nodeID, startVertexID, endVertexID, sizeOfPartition);
        }
    }

}
