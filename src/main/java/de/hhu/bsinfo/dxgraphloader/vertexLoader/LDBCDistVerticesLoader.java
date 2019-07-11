package de.hhu.bsinfo.dxgraphloader.vertexLoader;

import de.hhu.bsinfo.dxgraphloader.messages.GraphloadingMessages;
import de.hhu.bsinfo.dxgraphloader.messages.StartEndVerticesMessage;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.NodeIDNotExistException;
import de.hhu.bsinfo.dxgraphloader.util.Util;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.Vertex;
import de.hhu.bsinfo.dxgraphloader.vertexLoader.model.VertexLoader;
import de.hhu.bsinfo.dxnet.MessageReceiver;
import de.hhu.bsinfo.dxnet.core.Message;
import de.hhu.bsinfo.dxnet.core.NetworkException;
import de.hhu.bsinfo.dxram.chunk.ChunkLocalService;
import de.hhu.bsinfo.dxram.chunk.ChunkService;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierID;
import de.hhu.bsinfo.dxram.lookup.overlay.storage.BarrierStatus;
import de.hhu.bsinfo.dxram.nameservice.NameserviceService;
import de.hhu.bsinfo.dxram.net.NetworkService;
import de.hhu.bsinfo.dxram.sync.SynchronizationService;
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
import java.util.concurrent.TimeUnit;

import static de.hhu.bsinfo.dxgraphloader.GraphLoader.GLOBAL_META_DATA;

public class LDBCDistVerticesLoader extends VertexLoader implements MessageReceiver {

    private final Logger LOGGER = LogManager.getFormatterLogger(LDBCDistVerticesLoader.class);

    private final String MESSAGE_BARRIER = "BMSG";
    private final String UPDATE_BARRIER = "BU";

    private ChunkLocalService localService;
    private NetworkService networkService;
    private SynchronizationService syncService;
    private ChunkService chunkService;
    private NameserviceService nameService;

    private short currentNodeID;
    private boolean isCoordinator;
    private short coordinatorID;

    private LoadingMetaData metaData;


    public LDBCDistVerticesLoader(short currentNodeID, boolean isCoordinator, short coordinatorID, ChunkLocalService chunkLocalService, ChunkService chunkService, NameserviceService nameService, NetworkService networkService, SynchronizationService synchronizationService) {
        super();
        this.localService = chunkLocalService;
        this.networkService = networkService;
        this.syncService = synchronizationService;
        this.chunkService = chunkService;
        this.nameService = nameService;

        this.currentNodeID = currentNodeID;
        this.isCoordinator = isCoordinator;
        this.coordinatorID = coordinatorID;
    }


    @Override
    public LoadingMetaData loadVertices(String filePath, LoadingMetaData metaData) {
        this.metaData = metaData;
        this.initVertexContainer(metaData.getNumOfVertices());
        registerMessageTypeAndReceiver();
        this.metaData = this.loadVerticesFile(filePath, metaData);


        try {
            slavesSendResultCoordinatorProcesses();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return this.metaData;
    }

    private LoadingMetaData loadVerticesFile(String filePath, LoadingMetaData metaData) {
        try (final BufferedReader br = new BufferedReader(
                new InputStreamReader(
                        new BufferedInputStream(
                                Files.newInputStream(Paths.get(filePath), StandardOpenOption.READ),
                                1000000),
                        StandardCharsets.US_ASCII))) {
            String line = null;
            final int outMod = 1000000;
            int cntVertices = 0;
            int lineNumber = 1;
            int numberOfVerticesOfCurrentNode = metaData.getNumOfVerticesOfNode(this.currentNodeID);
            Vertex v = new Vertex();
            LOGGER.info("Node %d: Create ID space for %d objects", this.currentNodeID, numberOfVerticesOfCurrentNode);

            long[] ids = new long[numberOfVerticesOfCurrentNode];
            localService.createLocal().create(ids, numberOfVerticesOfCurrentNode, v.sizeofObject(), true); //true = aufsteigend
            final long firstId = ids[0];
            long chunkId = firstId;
            ids = null;
            System.gc();
            System.out.println("");
            LOGGER.info("Node %d: ID space created", this.currentNodeID);
            LOGGER.info("Node %d: Start processing vertex file", this.currentNodeID);
            LOGGER.info("Node %d: Startline: %d Endline: %d", this.currentNodeID,
                    metaData.getStartLineNumberOfVerticesFile(this.currentNodeID), metaData.getEndLineNumberOfVerticesFile(this.currentNodeID));
            while ((line = br.readLine()) != null) {

                if (!isLineToConsider(lineNumber, metaData)) {
                    lineNumber++;
                    continue;
                }

                if (isLineToConsider(lineNumber, metaData)) {
                    if (cntVertices == 0) {
                        LOGGER.info("Node %d: First relevant line: %d", this.currentNodeID, lineNumber);
                    }

                    if (cntVertices == numberOfVerticesOfCurrentNode - 1) {
                        LOGGER.info("Node %d: Last relevant line: %d", this.currentNodeID, lineNumber);

                    }
                }
                lineNumber++;

                long vid = Long.parseLong(line.split("\\s")[0]);
                if (cntVertices == 0) {
                    LOGGER.info("Node %d: Startvertex: %d", this.currentNodeID, vid);
                    metaData.changeStartVertexIDOfNode(this.currentNodeID, vid, chunkId);
                }
                if (cntVertices == numberOfVerticesOfCurrentNode - 1) {
                    LOGGER.info("Node %d: Endvertex: %d", this.currentNodeID, vid);
                    metaData.changeEndVertexIDOfNode(this.currentNodeID, vid, chunkId);
                }
                v.setID(chunkId++);
                v.setExternalId(vid);

                this.idMapper.put(vid, (int) ((v.getID() - firstId) & 0x0000ffffffffffffL));
                this.addVertex(v);
                cntVertices++;
                if (cntVertices % outMod == 0) {
                    LOGGER.info("Node %d: Processing %dM vertices finished...", this.currentNodeID, (cntVertices / outMod));
                }
            }
            LOGGER.info("Node %d: Processing vertices finished", this.currentNodeID);

        } catch (IOException | NodeIDNotExistException e) {
            e.printStackTrace();
        }
        return metaData;
    }

    private boolean isLineToConsider(int lineNumber, LoadingMetaData metaData) throws NodeIDNotExistException {
        int firstLineNumber = metaData.getStartLineNumberOfVerticesFile(this.currentNodeID);
        int lastLineNumber = metaData.getEndLineNumberOfVerticesFile(this.currentNodeID);
        return lineNumber >= firstLineNumber && lineNumber <= lastLineNumber;
    }

    private void slavesSendResultCoordinatorProcesses() throws InterruptedException {
        if (!this.isCoordinator) {
            try {
                LOGGER.info("Slave %d: Sending vertices metadata result to coordinator", currentNodeID);
                StartEndVerticesMessage msg = new StartEndVerticesMessage(this.coordinatorID,
                        metaData.getStartInternalVertexIDOfNode(this.currentNodeID),
                        metaData.getStartExternalVertexIDOfNode(this.currentNodeID),
                        metaData.getEndInternalVertexIDOfNode(this.currentNodeID),
                        metaData.getEndExternalVertexIDOfNode(this.currentNodeID));
                this.networkService.sendMessage(msg);
                LOGGER.info("Slave %d: Message has been send", this.currentNodeID);
                LOGGER.info("Slave %d: Getting vertex barrier id", currentNodeID);

                Util.waitForAllBarrier(false, BarrierID.INVALID_ID, MESSAGE_BARRIER, false, syncService, nameService);

                Util.waitForAllBarrier(false, BarrierID.INVALID_ID, UPDATE_BARRIER, syncService, nameService);

            } catch (NodeIDNotExistException e) {
                e.printStackTrace();
            } catch (NetworkException e) {
                e.printStackTrace();
            }
        } else {
            LOGGER.info("Coordinator %d: Creating vertex barrier with size %d", this.currentNodeID, metaData.getNumberOfNodes());

            int barrierID = Util.buildBarrier(MESSAGE_BARRIER, metaData.getNumberOfNodes(), syncService, nameService);

            Util.waitForAllBarrier(true, barrierID, MESSAGE_BARRIER, true, syncService, nameService);

            barrierID = Util.buildBarrier(UPDATE_BARRIER, metaData.getNumberOfNodes(), syncService, nameService);

            LOGGER.info("Update global meta data...");
            if (!this.chunkService.put().put(this.metaData)) {
                LOGGER.error("Coordinator: Updating failed for new global metadata");
                System.exit(1);
            }
            TimeUnit.SECONDS.sleep(1);
            LOGGER.info("Update done!");
            nameService.register(metaData.getID(), GLOBAL_META_DATA);
            this.syncService.barrierSignOn(barrierID, 0, true);
            this.syncService.barrierFree(barrierID);

            LOGGER.info("Coodinator %d: Updated global metadata. Now free barrier!", this.currentNodeID);
        }
    }

    private void registerMessageTypeAndReceiver() {
        LOGGER.info("%d: Register Message type", this.currentNodeID);
        this.networkService.registerMessageType(GraphloadingMessages.GRAPHLOADING_VERTICES_MESSAGE_TYPE,
                GraphloadingMessages.SUBTYPE_Graphloading_VERTICES_RESPONSE, StartEndVerticesMessage.class);
        if (isCoordinator) {
            LOGGER.info("%d: Register as receiver", this.currentNodeID);
            this.networkService.registerReceiver(GraphloadingMessages.GRAPHLOADING_VERTICES_MESSAGE_TYPE,
                    GraphloadingMessages.SUBTYPE_Graphloading_VERTICES_RESPONSE, this);
        }
    }


    @Override
    public void onIncomingMessage(Message p_message) {
        LOGGER.info("Coordinator %d: Receiving Message from %d", this.currentNodeID, p_message.getSource());
        if (p_message.getType() == GraphloadingMessages.GRAPHLOADING_VERTICES_MESSAGE_TYPE &&
                p_message.getSubtype() == GraphloadingMessages.SUBTYPE_Graphloading_VERTICES_RESPONSE) {
            StartEndVerticesMessage msg = (StartEndVerticesMessage) p_message;
            try {
                this.metaData.changeStartVertexIDOfNode(msg.getSource(), msg.getStartExternalVertexID(), msg.getStartInternalVertexID());
                this.metaData.changeEndVertexIDOfNode(msg.getSource(), msg.getEndExternalVertexID(), msg.getEndInternalVertexID());
                LOGGER.info("Coordinator %d: Changed Start- and Endvertices from %d", this.currentNodeID, msg.getSource());
            } catch (NodeIDNotExistException e) {
                e.printStackTrace();
            }
        }

    }

}
