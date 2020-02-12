package com.github.hermannpencole.nifi.config.service;

import com.github.hermannpencole.nifi.swagger.ApiClient;
import com.github.hermannpencole.nifi.swagger.ApiException;
import com.github.hermannpencole.nifi.swagger.Configuration;
import com.github.hermannpencole.nifi.swagger.client.AccessApi;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * Created by SFRJ2737 on 2017-05-28.
 *
 * @author hermann pencolé
 */
@Service
public class AccessService {

    @Autowired
    private AccessApi apiInstance;

    /**
     * add token on http client. The token is ask to nifi.
     *
     * @param accessFromTicket
     * @param username
     * @param password
     * @throws ApiException
     */
    public void addTokenOnConfiguration(boolean accessFromTicket, String username, String password) throws ApiException {
        ApiClient client = Configuration.getDefaultApiClient();
        if (accessFromTicket) {
            String token = apiInstance.createAccessTokenFromTicket();
            client.setAccessToken(token);
        } else if (username != null) {
            String token = apiInstance.createAccessToken(username, password);
            client.setAccessToken(token);
        }
        Configuration.setDefaultApiClient(client);
    }

    /**
     * Configure the default http client
     *
     * @param basePath
     * @param verifySsl
     * @param debugMode
     * @param connectionTimeout
     * @throws ApiException
     */
    public void setConfiguration(String basePath, boolean verifySsl, boolean debugMode, int connectionTimeout) {
        ApiClient client = Configuration.getDefaultApiClient()
                .setBasePath(basePath)
                .setVerifyingSsl(verifySsl)
                .setConnectTimeout(connectionTimeout)
                .setDebugging(debugMode);
        Configuration.setDefaultApiClient(client);
    }

}
