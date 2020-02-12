package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.config.model.Connection;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.ConnectionsApi;
import com.github.hermannpencole.nifi.swagger.client.model.ConnectionDTO;
import com.github.hermannpencole.nifi.swagger.client.model.ConnectionEntity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class ConnectionsUpdater {

    @Autowired
    private ConnectionsApi connectionsApi;

    public void updateConnections(List<Connection> connectionsConfiguration, List<ConnectionEntity> currentConnections) {

        Map<String, Connection> connectionMap = connectionsConfiguration
                .stream()
                .collect(Collectors.toMap(Connection::getId, Function.identity()));

        currentConnections.forEach(
                entity -> {
                    ConnectionDTO connectionDTO = entity.getComponent();
                    Connection config = connectionMap.getOrDefault(connectionDTO.getId(), null);
                    if (config != null) {
                        connectionDTO.setBackPressureObjectThreshold(config.getBackPressureObjectThreshold());
                        connectionDTO.setBackPressureDataSizeThreshold(config.getBackPressureDataSizeThreshold());
                        connectionDTO.setSelectedRelationships(null);
                        try {
                            connectionsApi.updateConnection(entity.getId(), entity);
                        } catch (ApiException apie) {

                        }
                    }
                });
    }

}
