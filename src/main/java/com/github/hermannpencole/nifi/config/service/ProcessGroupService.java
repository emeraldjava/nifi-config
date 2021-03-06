package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.config.NifiClientProperties;
import com.github.hermannpencole.nifi.config.utils.FunctionUtils;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.FlowApi;
import com.github.hermannpencole.nifi.swagger.client.ProcessGroupsApi;
import com.github.hermannpencole.nifi.swagger.client.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

import static com.github.hermannpencole.nifi.config.utils.FunctionUtils.findByComponentName;

/**
 * Class that offer service for process group
 * <p>
 * Created by SFRJ on 01/04/2017.
 */
@Service
public class ProcessGroupService {

    /**
     * The logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(ProcessGroupService.class);
    private final static double ELEMENT_WIDTH = 430;
    private final static double ELEMENT_HEIGHT = 220;
    private final static double APPROXIMATE = 0.01;

    private final static String CONNECTION_EXCEPTION_REGEX = "Cannot delete Process Group because [a-zA-Z]+ " +
            "Port [a-zA-Z0-9-]+ has at least one [a-z]+ " +
            "connection [a-z]+ a component outside of the Process Group. Delete this connection first.";

    @Autowired
    private FlowApi flowapi;

    @Autowired
    private ProcessGroupsApi processGroupsApi;

    @Autowired
    private ProcessorService processorService;

    @Autowired
    private PortService portService;

    @Autowired
    private ConnectionService connectionService;

    //@Qualifier("placeWidth")
    //@Autowired
    public Double placeWidth = 0.0d;

    //@Qualifier("startPosition")
    //@Autowired
    public PositionDTO startPosition = FunctionUtils.createPosition("1,2");

    @Autowired
    private NifiClientProperties properties;
    /**
     * browse nifi on branch pass in parameter
     *
     * @param branch
     * @return Optional<ProcessGroupFlowEntity>
     * @throws ApiException
     */
    public Optional<ProcessGroupFlowEntity> changeDirectory(List<String> branch) throws ApiException {
        ProcessGroupFlowEntity flowEntity = flowapi.getFlow("root");
        for (String processGroupName : branch.subList(1, branch.size())) {
            Optional<ProcessGroupEntity> flowEntityChild = findByComponentName(flowEntity.getProcessGroupFlow().getFlow().getProcessGroups(), processGroupName);
            if (!flowEntityChild.isPresent()) {
                return Optional.empty();
            }
            flowEntity = flowapi.getFlow(flowEntityChild.get().getId());
        }
        return Optional.of(flowEntity);
    }

    /**
     * browse nifi on branch pass in parameter
     *
     * @param branch
     * @return
     * @throws ApiException
     */
    public ProcessGroupFlowEntity createDirectory(List<String> branch) throws ApiException {
        return this.createDirectory(branch, null);
    }

    /**
     * browse nifi on branch pass in parameter and create processgroup if necessary
     * @param branch
     * @param comments
     * @return
     * @throws ApiException
     */
    public ProcessGroupFlowEntity createDirectory(List<String> branch, String comments) throws ApiException {
        //generate clientID
        String clientId = flowapi.generateClientId();
        //find root
        ProcessGroupFlowEntity flowEntity = flowapi.getFlow("root");
        for (String processGroupName : branch.subList(1, branch.size())) {
            Optional<ProcessGroupEntity> flowEntityChild = findByComponentName(flowEntity.getProcessGroupFlow().getFlow().getProcessGroups(), processGroupName);
            if (!flowEntityChild.isPresent()) {
                PositionDTO position = getNextPosition(flowEntity);
                ProcessGroupEntity created = new ProcessGroupEntity();
                created.setRevision(new RevisionDTO());
                created.setComponent(new ProcessGroupDTO());
                created.getRevision().setVersion(0L);
                created.getRevision().setClientId(clientId);
                created.getComponent().setName(processGroupName);
                created.getComponent().setPosition(position);
                created = createProcessGroup(flowEntity.getProcessGroupFlow().getId(), created);
                if (comments != null) {
                    //add comment
                    created.getComponent().setComments(comments);
                    created = updateProcessGroup(created.getId(), created);
                }
                flowEntity = flowapi.getFlow(created.getId());
            } else {
                flowEntity = flowapi.getFlow(flowEntityChild.get().getId());
            }
        }
        return flowEntity;
    }

    /**
     *
     * @param id
     * @param entity
     * @return
     */
    public ProcessGroupEntity createProcessGroup(String id, ProcessGroupEntity entity) throws ApiException {
        return processGroupsApi.createProcessGroup(id, entity);
    }

    /**
     *
     * @param id
     * @param entity
     * @return
     */
    public ProcessGroupEntity updateProcessGroup(String id, ProcessGroupEntity entity) throws ApiException {
        return processGroupsApi.updateProcessGroup(id, entity);
    }

    /**
     * set state on entire process group (no report error if there is)
     *
     * @param id
     * @param state
     * @throws ApiException
     */
    public void setState(String id, ScheduleComponentsEntity.StateEnum state) throws ApiException {
        ScheduleComponentsEntity body = new ScheduleComponentsEntity();
        body.setId(id);
        body.setState(state);
        body.setComponents(null);//for all
        flowapi.scheduleComponents(id, body);
    }

    /**
     * start the processor group.
     * Begin by processor that consumme flow and end with processor that consumme stream and create flow
     *
     * @param processGroupFlow
     * @throws ApiException
     */
    public void start(ProcessGroupFlowEntity processGroupFlow) throws ApiException {
        try {
            List<Set<?>> listing = reorder(processGroupFlow.getProcessGroupFlow());
            for (int i = (listing.size() - 1); i >= 0; i--) {
                Set<?> set = listing.get(i);
                for (Object object : set) {
                    if (object instanceof ProcessorEntity) {
                        processorService.setState((ProcessorEntity) object, ProcessorDTO.StateEnum.RUNNING);
                    } else if (object instanceof PortEntity) {
                        portService.setState((PortEntity) object, PortDTO.StateEnum.STOPPED);
                    }
                }
            }
            for (ProcessGroupEntity procGroupInConf : processGroupFlow.getProcessGroupFlow().getFlow().getProcessGroups()) {
                ProcessGroupFlowEntity processGroupFlowEntity = flowapi.getFlow(procGroupInConf.getId());
                start(processGroupFlowEntity);
            }
            setState(processGroupFlow.getProcessGroupFlow().getId(), ScheduleComponentsEntity.StateEnum.RUNNING);
        } catch (Exception e) {
            setState(processGroupFlow.getProcessGroupFlow().getId(), ScheduleComponentsEntity.StateEnum.STOPPED);
            throw e;
        }
    }

    /**
     * stop the processor group.
     * Begin by processor that consumme stream and create flow and end with processor that consumme flow.
     *
     * @param processGroupFlow
     * @throws ApiException
     */
    public void stop(ProcessGroupFlowEntity processGroupFlow) throws ApiException {
        try {
            List<Set<?>> listing = reorder( processGroupFlow.getProcessGroupFlow());
            for (int i = 0; i < (listing.size()); i++) {
                Set<?> set = listing.get(i);

                if (set.size()>0 && set.stream().findFirst().get() instanceof ConnectionEntity) {
                    //make be sur that in one pass all queue are empty (for the case when there is cycle)
                    boolean emptyQueue = true;
                    do {
                        emptyQueue = true;
                        for (Object o : set) {
                            emptyQueue = emptyQueue && connectionService.isEmptyQueue((ConnectionEntity) o);
                        }
                        if (!emptyQueue) {
                            for (Object o : set) { connectionService.waitEmptyQueue((ConnectionEntity) o); }
                        }
                    } while (!emptyQueue);
                }
                //TODO manage remoteProcessGroup
                for (Object object : set) {
                    if (object instanceof ProcessorEntity) {
                        processorService.setState((ProcessorEntity) object, ProcessorDTO.StateEnum.STOPPED);
                    } else if (object instanceof PortEntity) {
                        portService.setState((PortEntity) object, PortDTO.StateEnum.STOPPED);
                    }
                }
            }
            for (ProcessGroupEntity procGroupInConf : processGroupFlow.getProcessGroupFlow().getFlow().getProcessGroups()) {
                ProcessGroupFlowEntity processGroupFlowEntity = flowapi.getFlow(procGroupInConf.getId());
                stop(processGroupFlowEntity);
            }
            setState(processGroupFlow.getProcessGroupFlow().getId(), ScheduleComponentsEntity.StateEnum.STOPPED);
        } catch (Exception e) {
            setState(processGroupFlow.getProcessGroupFlow().getId(), ScheduleComponentsEntity.StateEnum.RUNNING);
            throw e;
        }
    }

    /**
     * reorder for have the processor that consume stream -> connection -> processor connected etc ...in the good order.
     *
     * Just put the first at the first and the other after trick for bypass cycle
     *
     * @param processGroupFlow
     * @return
     */
    public List<Set<?>> reorder(ProcessGroupFlowDTO processGroupFlow) {
        List<Set<?>> level = new ArrayList<>();

        Set<ProcessGroupFlowDTO> allProcessGroupFlow = getAllProcessGroupFlow(processGroupFlow);
        Set<ConnectionEntity> allConnections = allProcessGroupFlow.stream().flatMap(p->p.getFlow().getConnections().stream()).collect(Collectors.toSet());
        Set<ProcessorEntity> allProcessors = allProcessGroupFlow.stream().flatMap(p->p.getFlow().getProcessors().stream()).collect(Collectors.toSet());

        //get the first
        Set<String> destination = new HashSet<>();
        Set<String> source = new HashSet<>();
        allProcessors.forEach( processor-> source.add(processor.getId()));
        allConnections.forEach( connection -> {
            destination.add(connection.getDestinationId());
            source.add(connection.getSourceId());
        });

        //get the first (the first have no destination)
        Set<String> first = new HashSet<>(source);
        first.removeAll(destination);
        level.add(first.stream().map(id -> findById(allProcessGroupFlow,id)).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toSet()));

        //get the other (the other have destination)
        level.add(allConnections);
        level.add(destination.stream().map(id -> findById(allProcessGroupFlow,id)).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toSet()));

        if (level.isEmpty()) {
            level.add(new HashSet<ProcessorEntity>());
        }
        return level;
    }

    private Set<ProcessGroupFlowDTO> getAllProcessGroupFlow(ProcessGroupFlowDTO processGroupFlow) { //throws ApiException {
        Set<ProcessGroupFlowDTO> result = new HashSet<>();
        result.add(processGroupFlow);
        for (ProcessGroupEntity processGroup : processGroupFlow.getFlow().getProcessGroups()) {
            try {
                result.add(flowapi.getFlow(processGroup.getId()).getProcessGroupFlow());
            } catch (ApiException apie){

            }
        }
        return result;
    }

    /**
     * find processor, inputport, ouput port funnel or remote processor by id in allProcessGroupFlow
     *
     * @param allProcessGroupFlow
     * @param id
     * @return
     */
    public Optional<?> findById(Set<ProcessGroupFlowDTO> allProcessGroupFlow, String id){
        for (ProcessGroupFlowDTO processGroupFlowDTO : allProcessGroupFlow) {
            Optional<?> result = findById(processGroupFlowDTO.getFlow(),id);
            if (result.isPresent()) {
                return result;
            }
        }
        return Optional.empty();
    }

    /**
     * find processor, inputport, ouput port funnel or remote processor by id in flow
     *
     * @param flow
     * @param id
     * @return
     */
    public Optional<?> findById(FlowDTO flow, String id){
        Optional<?> result = flow.getProcessors().stream().filter(processor -> id.equals(processor.getId())).findFirst();
        if (!result.isPresent())
            result = flow.getInputPorts().stream().filter(port -> id.equals(port.getId())).findFirst();
        if (!result.isPresent())
            result = flow.getOutputPorts().stream().filter(port -> id.equals(port.getId())).findFirst();
        if (!result.isPresent())
            result = flow.getFunnels().stream().filter(funnel -> id.equals(funnel.getId())).findFirst();
        if (!result.isPresent())
            result = flow.getRemoteProcessGroups().stream().filter(remoteProcessGroup -> id.equals(remoteProcessGroup.getId())).findFirst();
        return result;
    }

    public void delete(String processGroupId) throws ApiException {
        FunctionUtils.runWhile(() -> {
            ProcessGroupEntity processGroupToRemove = null;
            ProcessGroupEntity processGroupEntity = null;
            try {
                //the state change, then the revision also in nifi 1.3.0 (only?) reload processGroup
                processGroupEntity = processGroupsApi.getProcessGroup(processGroupId);
                processGroupToRemove = processGroupsApi.removeProcessGroup(processGroupId, processGroupEntity.getRevision().getVersion().toString(), null);
            } catch (ApiException e) {
                final String responseBody = e.getResponseBody();
                LOG.info(responseBody);
                if (responseBody != null && responseBody.matches(CONNECTION_EXCEPTION_REGEX) && processGroupEntity != null) {
                    try {
                        connectionService.removeExternalConnections(processGroupEntity);
                    } catch (ApiException apie){
                        apie.printStackTrace();
                    }
                    //continuing the loop
                    return true;
                }
                if (responseBody == null || !responseBody.endsWith("is running")) {
                    //throw e;
                    return true;
                }
            }
            return processGroupToRemove == null;
        }, properties.interval, properties.timeout);
    }

    /**
     * get the next free position to place the processor(or group processor) on this group processor
     *
     * @param flowEntity
     * @return
     */
    public PositionDTO getNextPosition(ProcessGroupFlowEntity flowEntity) {
        PositionDTO nextPosition = new PositionDTO();
        List<PositionDTO> positions = new ArrayList<>();
        for (ProcessorEntity processor : flowEntity.getProcessGroupFlow().getFlow().getProcessors()) {
            addPosition(positions, processor.getPosition());
        }
        for (ProcessGroupEntity processGroup : flowEntity.getProcessGroupFlow().getFlow().getProcessGroups()) {
            addPosition(positions, processGroup.getPosition());
        }
        for (PortEntity port : flowEntity.getProcessGroupFlow().getFlow().getInputPorts()) {
            addPosition(positions, port.getPosition());
        }
        for (PortEntity port : flowEntity.getProcessGroupFlow().getFlow().getOutputPorts()) {
            addPosition(positions, port.getPosition());
        }
        for (ConnectionEntity conn : flowEntity.getProcessGroupFlow().getFlow().getConnections()) {
            addPosition(positions, conn.getPosition());
        }
        for (FunnelEntity funnel : flowEntity.getProcessGroupFlow().getFlow().getFunnels()) {
            addPosition(positions, funnel.getPosition());
        }
        nextPosition.setX(startPosition.getX());
        nextPosition.setY(startPosition.getY());
        Optional<PositionDTO> otherPosition;
        Optional<PositionDTO> fistInLine = Optional.empty();
        while ( (otherPosition = findOtherPositionInPlace(positions, nextPosition)).isPresent() ) {
            if (!fistInLine.isPresent()) {
                fistInLine = otherPosition;
            }
            //plus 2* while 1 for the other and 1 for the element
            if (otherPosition.get().getX() + 2*ELEMENT_WIDTH >= placeWidth) {
                nextPosition.setX(0d);
                nextPosition.setY(fistInLine.get().getY() + ELEMENT_HEIGHT);
                fistInLine = Optional.empty();
            } else {
                nextPosition.setY(otherPosition.get().getY());
                nextPosition.setX(otherPosition.get().getX() + ELEMENT_WIDTH);
            }
        }
        LOG.debug("next postion {},{}", nextPosition.getX(), nextPosition.getY());
        return nextPosition;
    }

    public void addPosition( List<PositionDTO> positions, PositionDTO positionToAdd) {
        if (positionToAdd != null && positionToAdd.getX() != null && positionToAdd.getY() != null) {
            positions.add(positionToAdd);
        }
    }

    private Optional<PositionDTO> findOtherPositionInPlace(List<PositionDTO> positions, PositionDTO nextPosition) {
        return positions.stream().filter(position ->
                 nextPosition.getX() + (ELEMENT_WIDTH - APPROXIMATE) > position.getX() && nextPosition.getX()- (ELEMENT_WIDTH - APPROXIMATE) < position.getX()
              && nextPosition.getY() + (ELEMENT_HEIGHT - APPROXIMATE) > position.getY() && nextPosition.getY()- (ELEMENT_HEIGHT - APPROXIMATE) < position.getY()
        ).findFirst();
    }
}
