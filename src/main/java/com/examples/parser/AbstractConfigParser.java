package com.examples.parser;

import java.io.Serializable;
import java.util.Map;

public abstract class AbstractConfigParser implements Serializable {


    public AbstractConfigParser(Map<String, Object> map) {
        initConfig(map);
    }

    protected abstract void initConfig(Map<String, Object> map);
}
