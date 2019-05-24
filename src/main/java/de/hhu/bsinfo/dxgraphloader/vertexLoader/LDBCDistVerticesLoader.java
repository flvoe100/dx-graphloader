package de.hhu.bsinfo.dxgraphloader.vertexLoader;

import de.hhu.bsinfo.dxgraphloader.messages.GraphloadingMessages;
import de.hhu.bsinfo.dxgraphloader.messages.StartEndVerticesMessage;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.LoadingMetaData;
import de.hhu.bsinfo.dxgraphloader.metaDataLoader.model.NodeIDNotExistException;
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

public class LDBCDistVerticesLoader extends VertexLoader implements MessageReceiver {

    private final Logger LOGGER = LogManager.getFormatterLogger(LDBCDistVerticesLoader.class);

    private final String MESSAGE_BARRIER = "BMSG";
    private final String UPDATE_BARRIER = "BU";
    private final int TIMEOUT = 1000;


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
        registerMessageTypeAndReceiver();
        this.metaData = this.loadVerticesFile(filePath, metaData);


        slavesSendResultCoordinatorProcesses();

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
            int lineNumber = 0;
            int numberOfVerticesOfCurrentNode = metaData.getNumOfVerticesOfNode(this.currentNodeID);
            Vertex v = new Vertex();
            LOGGER.info("Node %d: Create ID space objects", numberOfVerticesOfCurrentNode);

            long[] ids = new long[numberOfVerticesOfCurrentNode];
            localService.createLocal().create(ids, numberOfVerticesOfCurrentNode, v.sizeofObject(), true); //true = aufsteigend
            final long firstId = ids[0];
            long chunkId = firstId;
            ids = null;
            System.gc();
            System.out.println("");
            LOGGER.info("Node %d: ID space created", numberOfVerticesOfCurrentNode);
            LOGGER.info("Node %d: Start processing vertex file", numberOfVerticesOfCurrentNode);
            while ((line = br.readLine()) != null) {

                if (isLineToConsider(lineNumber, metaData)) {
                    lineNumber++;
                    continue;
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

    private void slavesSendResultCoordinatorProcesses() {
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

                int barrierID = BarrierID.INVALID_ID;
                while (barrierID == BarrierID.INVALID_ID) {
                    barrierID = (int) this.nameService.getChunkID(this.MESSAGE_BARRIER, this.TIMEOUT);
                }
                LOGGER.info("Slave %d: Entering vertex barrier", this.currentNodeID);
                this.syncService.barrierSignOn(barrierID, 0, true);

                barrierID = BarrierID.INVALID_ID;
                while (barrierID == BarrierID.INVALID_ID) {
                    barrierID = (int) this.nameService.getChunkID(this.UPDATE_BARRIER, this.TIMEOUT);
                }
                LOGGER.info("Slave %d: Entering update barrier", this.currentNodeID);
                this.syncService.barrierSignOn(barrierID, 0, true);
            } catch (NodeIDNotExistException e) {
                e.printStackTrace();
            } catch (NetworkException e) {
                e.printStackTrace();
            }
        } else {
            LOGGER.info("Coordinator %d: Creating vertex barrier with size %d", this.currentNodeID, metaData.getNumberOfNodes() - 1);

            int barrierID = this.syncService.barrierAllocate(metaData.getNumberOfNodes());
            this.nameService.register(barrierID, this.MESSAGE_BARRIER);
            BarrierStatus bStatus = syncService.barrierSignOn(barrierID, 0, true);

            LOGGER.info("Coordinator %d: Checking if all slaves entered vertex barrier", this.currentNodeID);
            if (bStatus.getNumberOfSignedOnPeers() == metaData.getNumberOfNodes()) {
                this.syncService.barrierFree(barrierID);
                barrierID = this.syncService.barrierAllocate(metaData.getNumberOfNodes());
                this.nameService.register(barrierID, this.UPDATE_BARRIER);

                LOGGER.info("Coordinator %d: All slaves entered vertex barrier. Now update global metadata", this.currentNodeID);
                LOGGER.info("PUT");
                LOGGER.info(this.metaData);
                if(!this.chunkService.put().put(this.metaData)) {
                    LOGGER.error("Coordinator: Updating failed for new global metadata");
                }
                this.syncService.barrierSignOn(barrierID, 0, true);
                this.syncService.barrierFree(barrierID);
            }
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
