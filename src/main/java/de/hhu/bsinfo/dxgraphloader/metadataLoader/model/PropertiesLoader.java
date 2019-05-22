package de.hhu.bsinfo.dxgraphloader.metadataLoader.model;

import de.hhu.bsinfo.dxgraphloader.model.WrongGraphInputException;

public interface PropertiesLoader {

    LoadingMetaData loadProperties(String fileName, String datasetPrefix) throws WrongGraphInputException;

}
