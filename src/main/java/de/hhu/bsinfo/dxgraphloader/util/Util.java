package de.hhu.bsinfo.dxgraphloader.util;

import de.hhu.bsinfo.dxgraphloader.GraphLoader;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.NodeIDNotExistException;
import de.hhu.bsinfo.dxmem.data.ChunkID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Util {

    private static final Logger LOGGER = LogManager.getFormatterLogger(GraphLoader.class);
    public static int TIMEOUT = 1000;


    public static boolean isInRange(long start, long end, long toCheck) {
        return start <= toCheck && end <= end;
    }

    public static void waitForAllBarrier(boolean isCoordinator, int barrierID, String barrierString,  SynchronizationService syncService, NameserviceService nameService) {
        LOGGER.info("Trying to enter barrier %s", barrierString);

        LOGGER.info("Fetch barrier id");
        if (barrierID == BarrierID.INVALID_ID) {
            barrierID = (int) getIdFromNameService(barrierString, nameService);
        }
        LOGGER.info("Fetched barrier id");

        syncService.barrierSignOn(barrierID, 0, true);
        LOGGER.info("All got into barrier");

        if (isCoordinator) {
            syncService.barrierFree(barrierID);
            LOGGER.info("Coordinator freed barrier");
        }

    }

    public static void waitForAllBarrier(boolean isCoordinator, int barrierID, String barrierString, boolean shouldWait,  SynchronizationService syncService, NameserviceService nameService) {
        LOGGER.info("Trying to enter barrier %s", barrierString);

        LOGGER.info("Fetch barrier id");
        if (barrierID == BarrierID.INVALID_ID) {
            barrierID = (int) getIdFromNameService(barrierString, nameService);
        }
        LOGGER.info("Fetched barrier id");

        syncService.barrierSignOn(barrierID, 0, shouldWait);
        LOGGER.info("All got into barrier");

        if (isCoordinator) {
            syncService.barrierFree(barrierID);
            LOGGER.info("Coordinator freed barrier");
        }

    }

    public static int buildBarrier(String barrierString, int size, SynchronizationService syncService, NameserviceService nameService) {
        LOGGER.info("Create barrier");
        int barrierID = syncService.barrierAllocate(size);
        nameService.register(barrierID, barrierString);

        return barrierID;
    }

    public static long getIdFromNameService(String nameServiceString, NameserviceService nameService) {
        long dataID = ChunkID.INVALID_ID;
        LOGGER.info("Load %s", nameServiceString);
        while (dataID == ChunkID.INVALID_ID) {
            dataID = nameService.getChunkID(nameServiceString, TIMEOUT);
        }
        return dataID;
    }

    public static boolean isCoordinator(short ownID, short coordID) {
        return ownID == coordID;
    }
}
