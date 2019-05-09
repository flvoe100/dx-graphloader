package de.hhu.bsinfo.dxgraphloader.metaDataLoader.model;

import java.util.List;

public interface PropertiesLoader {

    public LoadingMetaData loadProperties(String fileName, List<Short> nodes) throws WrongGraphInputException;

}
