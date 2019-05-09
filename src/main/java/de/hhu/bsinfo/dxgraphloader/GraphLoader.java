package de.hhu.bsinfo.dxgraphloader;

import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.PropertiesLoader;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.VertexLoader;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import sun.net.spi.nameservice.NameService;

import java.util.List;

public class GraphLoader {

    private NameserviceService nameService;

    private PropertiesLoader propertiesLoader;
    private VertexLoader vertexLoader;
    private List<Short> nodes;
    private short ownID;

    private LoadingMetaData metaData;

    public GraphLoader(List<Short> nodes, short currentId, NameserviceService nameService, PropertiesLoader propertiesLoader, VertexLoader vertexLoader) {
        this.nameService = nameService;

        this.nodes = nodes;
        this.propertiesLoader = propertiesLoader;
        this.vertexLoader = vertexLoader;
        this.ownID = currentId;
    }

    public void loadGraph(String filePath) {
        if(this.nodes.get(0) == this.ownID) {
            metaData = this.propertiesLoader.loadProperties(filePath, this.nodes);
        }
    }

    private void loadProperties(){
        LoadingMetaData metaData = new LoadingMetaData();



        this.nameService.register(metaData, "Graphmetadata");
    }



}
