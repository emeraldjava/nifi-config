package com.github.hermannpencole.nifi.config;

import com.github.hermannpencole.nifi.config.service.CreateRouteService;
import com.github.hermannpencole.nifi.swagger.client.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NifiSwaggerClientConfig {

    @Bean
    public AccessApi getAccessApi(){
        return new AccessApi();
    }

    @Bean
    public FlowApi getFlowApi(){
        return new FlowApi();
    }

    @Bean
    public ConnectionsApi getConnectionsApi(){
        return new ConnectionsApi();
    }

    @Bean
    public FlowfileQueuesApi getFlowfileQueuesApi(){
        return new FlowfileQueuesApi();
    }

    @Bean
    public ProcessGroupsApi getProcessGroupsApi(){
        return new ProcessGroupsApi();
    }

    @Bean
    public ProcessorsApi getProcessorsApi(){
        return new ProcessorsApi();
    }

    @Bean
    public InputPortsApi getInputPortsApi() {
        return new InputPortsApi();
    }

    @Bean
    public OutputPortsApi getOutputPortsApi() {
        return new OutputPortsApi();
    }

    @Bean
    public ControllerServicesApi getControllerServicesApi() {
        return new ControllerServicesApi();
    }

    @Bean
    public TemplatesApi getTemplatesApi() {
        return new TemplatesApi();
    }

    @Bean
    public CreateRouteService getCreateRouteService() {
        return new CreateRouteService();
    }
}
