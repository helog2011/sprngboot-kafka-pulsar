package com.bap.kafka.context;

import com.bap.kafka.service.IPulsarService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * @author: heliang
 * @email heliang3019@163.com
 * @date: 2021/4/25 16:54
 */
@Service
public class PulsarContext {
    @Autowired
    private Map<String, IPulsarService> iPulsarServiceMap;

    public IPulsarService getIPulsarServiceMap(String contextName) {
        return iPulsarServiceMap.get(contextName);
    }

}
