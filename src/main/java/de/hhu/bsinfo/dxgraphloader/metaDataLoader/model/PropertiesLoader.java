package de.hhu.bsinfo.dxgraphloader.metaDataLoader.model;

import java.util.List;

public interface PropertiesLoader {

    public LoadingMetaData loadProperties(String fileName, String datasetPrefix, List<Short> peers) throws WrongGraphInputException;

}
