package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.config.NifiClientProperties;
import com.github.hermannpencole.nifi.config.model.ConfigException;
import com.github.hermannpencole.nifi.config.utils.FunctionUtils;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.ProcessorsApi;
import com.github.hermannpencole.nifi.swagger.client.model.ProcessorDTO;
import com.github.hermannpencole.nifi.swagger.client.model.ProcessorEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

/**
 * Class that offer service for process group
 * <p>
 * Created by SFRJ on 01/04/2017.
 */
@Service
public class ProcessorService {

    /**
     * The logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(ProcessorService.class);

    @Autowired
    private NifiClientProperties properties;

    @Autowired
    private ProcessorsApi processorsApi;

    /**
     * the the state of processor
     *
     * @param processor
     * @param state
     */
    public void setState(ProcessorEntity processor, ProcessorDTO.StateEnum state) { //throws ApiException {
        //how obtain state of and don't have this bullshit trick
        //trick for don't have error : xxxx cannot be started because it is not stopped. Current state is STOPPING
        if (processor.getComponent().getState().equals(ProcessorDTO.StateEnum.DISABLED)) {
            LOG.info(" {} ({}) is already disabled nifi-config make no update", processor.getComponent().getName() ,processor.getId());
            return;
        }
        boolean isReallyStopped = isReallyStopped(processor);
        if ((state.equals(ProcessorDTO.StateEnum.STOPPED) && state.equals(processor.getComponent().getState()) && isReallyStopped)
                || (state.equals(ProcessorDTO.StateEnum.RUNNING) && state.equals(processor.getComponent().getState()) ) ) {
            LOG.info(" {} ({}) is already {}", processor.getComponent().getName() ,processor.getId(), processor.getComponent().getState());
            return;
        }

        if (state.equals(ProcessorDTO.StateEnum.STOPPED) && state.equals(processor.getComponent().getState()) && !isReallyStopped) {
            //no update just waiting
        } else {
            try {
                ProcessorEntity body = new ProcessorEntity();
                body.setRevision(processor.getRevision());
                body.setComponent(new ProcessorDTO());
                body.getComponent().setState(state);
                body.getComponent().setId(processor.getId());
                body.getComponent().setRestricted(null);
                LOG.info(" {} ({}) update for {}", processor.getComponent().getName() ,processor.getId(), state);
                ProcessorEntity processorEntity = processorsApi.updateProcessor(processor.getId(), body);
                processor.setRevision(processorEntity.getRevision());
            } catch (ApiException e) {
                if (e.getResponseBody() == null || !e.getResponseBody().endsWith("Current state is STOPPING")) {
                    logErrors(processor);
                    throw new ConfigException(e.getMessage() + ": " + e.getResponseBody(), e);
                }
                LOG.info(e.getResponseBody());
            }
        }




        FunctionUtils.runWhile(()-> {
            LOG.info(" {} ({}) waiting for {}", processor.getComponent().getName() ,processor.getId(), state);

            ProcessorEntity processorEntity = null;
            try {
                processorEntity = processorsApi.getProcessor(processor.getId());
            } catch(ApiException a) {
                a.printStackTrace();
                return false;
            }


            boolean reallyStopped = isReallyStopped(processorEntity);
            LOG.info(" {} ({}) is {} (have thread active : {}) ", processorEntity.getComponent().getName(), processorEntity.getId(), processorEntity.getComponent().getState(), !reallyStopped);
            if ( (state.equals(ProcessorDTO.StateEnum.STOPPED) && state.equals(processorEntity.getComponent().getState()) && isReallyStopped(processorEntity))
                || (state.equals(ProcessorDTO.StateEnum.RUNNING) && state.equals(processorEntity.getComponent().getState())) ) {
                return false;
            }
            return true;
        }, properties.interval, properties.timeout);


    }

    /**
     * is really stopped when there are no active thread
     * @param processor
     * @return
     */
    private boolean isReallyStopped(ProcessorEntity processor) {
        return processor.getStatus() == null || processor.getStatus().getAggregateSnapshot() == null || processor.getStatus().getAggregateSnapshot().getActiveThreadCount() == null
                || processor.getStatus().getAggregateSnapshot().getActiveThreadCount() == 0;
    }

    /**
     * log the error reported by processor
     *
     * @param processor
     */
    private void logErrors(ProcessorEntity processor) {
        try {
            ProcessorEntity procInError = processorsApi.getProcessor(processor.getId());
            procInError.getComponent().getValidationErrors().stream().forEach(msg -> LOG.error(msg));
        } catch (ApiException e1) {
            LOG.error(e1.getMessage());
        }
    }

    public ProcessorEntity getById(String id) throws ApiException {
        return processorsApi.getProcessor(id);
    }
}
