package com.github.hermannpencole.nifi.config;

import com.github.hermannpencole.nifi.config.model.ConfigException;
import com.github.hermannpencole.nifi.config.service.AccessService;
import com.github.hermannpencole.nifi.config.service.InformationService;
import com.github.hermannpencole.nifi.config.service.ProcessGroupService;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.model.ProcessGroupFlowEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.ComponentScans;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScans({
  @ComponentScan("com.github.hermannpencole.nifi.config"),
  @ComponentScan("com.github.hermannpencole.nifi.swagger")
})
public class NifiApiClient {

    private final static Logger LOG = LoggerFactory.getLogger(NifiApiClient.class);

    @Autowired
    private NifiClientProperties properties;

    @Autowired
    private AccessService accessService;

    @Autowired
    private InformationService infoService;

    @Autowired
    private ProcessGroupService processGroupService;

    public void connect() throws ApiException {
        LOG.info("URL "+properties.url);
        accessService.setConfiguration(properties.url,
                properties.verifySsl,
                properties.debugMode,
                properties.timeout);
        accessService.addTokenOnConfiguration(false,properties.username, properties.password);
        String nifiVersion = infoService.getVersion();

        LOG.info(String.format("Starting config_nifi %s on mode %s", nifiVersion, properties.command));
    }

    public ProcessGroupService getProcessGroupService() {
        return processGroupService;
    }

    public ProcessGroupFlowEntity getProcessGroupFlow() throws ConfigException, ApiException {
        LOG.info("loading flow "+properties.branch);
        return processGroupService.createDirectory(properties.getBranchList());
    }

    public void stopProcessGroupFlow() throws ConfigException, ApiException {
        LOG.info("stop flow "+properties.branch);
        ProcessGroupFlowEntity processGroupFlowEntity = processGroupService.createDirectory(properties.getBranchList());
        processGroupService.stop(processGroupFlowEntity);
    }

    public void startProcessGroupFlow() throws ConfigException, ApiException {
        LOG.info("start flow "+properties.branch);
        ProcessGroupFlowEntity processGroupFlowEntity = processGroupService.createDirectory(properties.getBranchList());
        processGroupService.start(processGroupFlowEntity);
    }
}
