package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.config.model.ConfigException;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.FlowApi;
import com.github.hermannpencole.nifi.swagger.client.ProcessorsApi;
import com.github.hermannpencole.nifi.swagger.client.model.*;
import com.google.gson.Gson;
import com.github.hermannpencole.nifi.config.model.GroupProcessorsEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.io.*;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

/**
 * Class that offer service for nifi processor
 * <p>
 * Created by SFRJ on 01/04/2017.
 */
@Singleton
public class UpdateProcessorService {


    /**
     * The logger.
     */
    private final static Logger LOG = LoggerFactory.getLogger(UpdateProcessorService.class);

    @Inject
    private ProcessGroupService processGroupService;

    @Inject
    private FlowApi flowapi;

    @Inject
    private ProcessorsApi processorsApi;


    /**
     * @param branch
     * @param fileConfiguration
     * @throws IOException
     * @throws URISyntaxException
     * @throws ApiException
     */
    public void updateByBranch(List<String> branch, String fileConfiguration) throws IOException, ApiException {
        File file = new File(fileConfiguration);
        if (!file.exists()) {
            throw new FileNotFoundException("Repository " + file.getName() + " is empty or doesn't exist");
        }

        LOG.info("Processing : " + file.getName());
        Gson gson = new Gson();
        try (Reader reader = new InputStreamReader(new FileInputStream(file), "UTF-8")) {
            GroupProcessorsEntity configuration = gson.fromJson(reader, GroupProcessorsEntity.class);
            ProcessGroupFlowEntity componentSearch = processGroupService.changeDirectory(branch)
                    .orElseThrow(() -> new ConfigException(("cannot find " + Arrays.toString(branch.toArray()))));

            //Stop branch
            processGroupService.setState(componentSearch.getProcessGroupFlow().getId(), ScheduleComponentsEntity.StateEnum.STOPPED);
            LOG.info(Arrays.toString(branch.toArray()) + " is stopped");
            updateComponent(configuration, componentSearch);

            //Run all nifi processors
            processGroupService.setState(componentSearch.getProcessGroupFlow().getId(), ScheduleComponentsEntity.StateEnum.RUNNING);
            LOG.info(Arrays.toString(branch.toArray()) + " is running");
        }

    }

    /**
     * @param configuration
     * @param componentSearch
     * @throws ApiException
     */
    private void updateComponent(GroupProcessorsEntity configuration, ProcessGroupFlowEntity componentSearch) throws ApiException {
        FlowDTO flow = componentSearch.getProcessGroupFlow().getFlow();
        configuration.getProcessors().forEach(processorOnConfig -> updateProcessor(flow.getProcessors(), processorOnConfig));
        for (GroupProcessorsEntity procGroupInConf : configuration.getGroupProcessorsEntity()) {
            ProcessGroupEntity processorGroupToUpdate = ProcessGroupService.findByComponentName(flow.getProcessGroups(), procGroupInConf.getName())
                    .orElseThrow(() -> new ConfigException(("cannot find " + procGroupInConf.getName())));
            updateComponent(procGroupInConf, flowapi.getFlow(processorGroupToUpdate.getId()));
        }
    }

    /**
     * update processor configuration with valueToPutInProc
     * at first find id of each processor and in second way update it
     *
     * @param processorsList
     * @param componentToPutInProc
     */
    private void updateProcessor(List<ProcessorEntity> processorsList, ProcessorDTO componentToPutInProc) {
        try {
            ProcessorEntity processorToUpdate = findProcByComponentName(processorsList, componentToPutInProc.getName());
            componentToPutInProc.setId(processorToUpdate.getId());
            LOG.info("Update processor : " + processorToUpdate.getComponent().getName());
            //update on nifi
            processorToUpdate.setComponent(componentToPutInProc);
            processorsApi.updateProcessor(processorToUpdate.getId(), processorToUpdate);

            //nifiService.updateProcessorProperties(toUpdate, componentToPutInProc.getString("id"));
            LOG.info("Updated : " + componentToPutInProc.getName());
        } catch (ApiException e) {
            throw new ConfigException(e.getMessage() + ": " + e.getResponseBody(), e);
        }
    }

    //can static => utils
    public static ProcessorEntity findProcByComponentName(List<ProcessorEntity> listGroup, String name) {
        return listGroup.stream()
                .filter(item -> item.getComponent().getName().trim().equals(name.trim()))
                .findFirst().orElseThrow(() -> new ConfigException(("cannot find " + name)));
    }

}
