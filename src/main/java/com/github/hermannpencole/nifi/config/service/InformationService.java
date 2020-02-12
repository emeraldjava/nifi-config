package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.client.FlowApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Service;

/**
 * Created by SFRJ2737 on 2017-05-28.
 *
 * @author hermann pencolé
 */
@Service
public class InformationService {

    @Autowired
    private FlowApi flowApi;

    /**
     * get the nifi version.
     *
     * @throws ApiException
     */
    public String getVersion() throws ApiException {
        return flowApi.getAboutInfo().getAbout().getVersion();
    }
}
