package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.config.NifiClientProperties;
import com.github.hermannpencole.nifi.config.utils.FunctionUtils;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.ConnectionsApi;
import com.github.hermannpencole.nifi.swagger.client.FlowApi;
import com.github.hermannpencole.nifi.swagger.client.FlowfileQueuesApi;
import com.github.hermannpencole.nifi.swagger.client.ProcessGroupsApi;
import com.github.hermannpencole.nifi.swagger.client.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * Class that offer service for process group
 * <p>
 * Created by SFRJ on 01/04/2017.
 */
@Service
public class ConnectionService {

    @Autowired
    private NifiClientProperties properties;

    /**
     * The logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(ConnectionService.class);

    @Autowired
    private ConnectionsApi connectionsApi;

    @Autowired
    private FlowfileQueuesApi flowfileQueuesApi;

    @Autowired
    private FlowApi flowApi;

    @Autowired
    private ProcessGroupsApi processGroupsApi;

    @Autowired
    private ProcessorService processorService;

    @Autowired
    private PortService portService;

    private boolean stopProcessorOrPort(String id) throws ApiException {
        ProcessorEntity processorEntity = null;
        //try {
            processorEntity = processorService.getById(id);
        //} catch (ApiException e) {
            //do nothing
        //}
        if (processorEntity != null) {
            processorService.setState(processorEntity, ProcessorDTO.StateEnum.STOPPED);
            return true;
        }

        PortEntity portEntity = null;
        try {
            portEntity = portService.getById(id, PortDTO.TypeEnum.INPUT_PORT);
        } catch (ApiException e) {
            try {
                portEntity = portService.getById(id, PortDTO.TypeEnum.OUTPUT_PORT);
            } catch (ApiException e2) {
                LOG.info("Couldn't find processor or port to stop for id ({}).", id);
                return false;
            }
        }
        if (portEntity != null) {
            portService.setState(portEntity, PortDTO.StateEnum.STOPPED);
        }

        return false;
    }

    public boolean isEmptyQueue(ConnectionEntity connectionEntity) throws ApiException {
        return connectionsApi.getConnection(connectionEntity.getId()).getStatus().getAggregateSnapshot().getQueuedCount().equals("0");
    }

    public void waitEmptyQueue(ConnectionEntity connectionEntity) throws ApiException {
//        try {
            FunctionUtils.runWhile(() -> {
                ConnectionEntity connection = getConnectionEntity(connectionEntity.getId());
                LOG.info(" {} : there is {} FlowFile ({} bytes) on the queue ", connection.getId(), connection.getStatus().getAggregateSnapshot().getQueuedCount(), connection.getStatus().getAggregateSnapshot().getQueuedSize());
                return !connection.getStatus().getAggregateSnapshot().getQueuedCount().equals("0");
            }, properties.interval, properties.timeout);
  //      } catch (ApiException e) {
            //empty queue if forced mode
            if (properties.forceMode) {
                DropRequestEntity dropRequest = flowfileQueuesApi.createDropRequest(connectionEntity.getId());
                FunctionUtils.runWhile(() -> {
                    DropRequestEntity drop = getDropEntity(connectionEntity.getId(), dropRequest.getDropRequest().getId());
                    return !drop.getDropRequest().getFinished();
                }, properties.interval, properties.timeout);
                LOG.info(" {} : {} FlowFile ({} bytes) were removed from the queue", connectionEntity.getId(), dropRequest.getDropRequest().getCurrentCount(), dropRequest.getDropRequest().getCurrentSize());
                flowfileQueuesApi.removeDropRequest(connectionEntity.getId(), dropRequest.getDropRequest().getId());
            }
//            else {
//                LOG.error(e.getMessage(), e);
//                throw e;
//            }
        //}
    }

    private ConnectionEntity getConnectionEntity(String connectionEntityId) {
        try {
            return connectionsApi.getConnection(connectionEntityId);
        } catch (ApiException apie){
            LOG.error("ConnectionService.getConnectionEntity()",apie.getMessage());
            return null;
        }
    }

    private DropRequestEntity getDropEntity(String connectionEntityId, String dropRequestId) {
        try {
            return flowfileQueuesApi.getDropRequest(connectionEntityId, dropRequestId);
        } catch (ApiException apie) {
            LOG.error("ConnectionService.getDropEntity()",apie.getMessage());
            return null;
        }
    }

    public void removeExternalConnections(ProcessGroupEntity processGroupEntity)
            throws ApiException {
        final String groupId = processGroupEntity.getComponent().getId();

        ProcessGroupFlowEntity flow = flowApi.getFlow(groupId);
        final List<ConnectionEntity> groupConnections = processGroupsApi.getConnections(
                flow.getProcessGroupFlow().getParentGroupId()).getConnections();

        groupConnections.forEach(connection -> {
            if (connection.getDestinationGroupId().equals(groupId) || connection.getSourceGroupId().equals(groupId)) {
                //stopping source/destination
                if (connection.getDestinationGroupId().equals(groupId)) {
                    try {
                        stopProcessorOrPort(connection.getSourceId());
                    } catch (ApiException apie){
                        apie.printStackTrace();
                    }
                }
                if (connection.getSourceGroupId().equals(groupId)) {
                    try {
                        stopProcessorOrPort(connection.getDestinationId());
                    } catch (ApiException apie){
                        apie.printStackTrace();
                    }
                }

                try {
                    connectionsApi.deleteConnection(
                            connection.getComponent().getId(),
                            connection.getRevision().getVersion().toString(),
                            flowApi.generateClientId());
                } catch (ApiException apie){
                    apie.printStackTrace();
                }
            }
        });
    }

}
