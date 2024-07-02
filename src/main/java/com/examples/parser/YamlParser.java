package com.examples.parser;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Map;

public class YamlParser {
    final public String jobName;
    final private Map<String, Map<String, Object>> sources;
    final private Map<String, Map<String, Object>> sinks;

    public YamlParser(InputStream in) throws Exception {
        Map<String, Object> yamlConfig = new Yaml().load(in);
        if (yamlConfig.get("jobName") == null) {
            throw new Exception("Should provide jobName");
        }

        this.jobName = (String) yamlConfig.get("jobName");
        this.sources = (Map<String, Map<String, Object>>) yamlConfig.get("source");
        this.sinks = (Map<String, Map<String, Object>>) yamlConfig.get("sink");
    }

    public <T extends AbstractConfigParser> T getSourceConfig(Class<T> clazz, String key) throws Exception {
        // check sources
        if (sources == null) throw new Exception("Can not find source configuration");

        // check source yamlConfig
        Map<String, Object> yamlConfig = sources.get(key);
        if (yamlConfig == null) throw new Exception("Can not find " + key + " in source configuration");

        Constructor<T> constructor = clazz.getDeclaredConstructor(Map.class);

        return constructor.newInstance(yamlConfig);
    }

    public <T extends AbstractConfigParser> T getSinkConfig(Class<T> clazz, String key) throws Exception {
        // check sinks
        if (sinks == null) throw new Exception("Can not find sink configuration");

        // check sink yamlConfig
        Map<String, Object> yamlConfig = sinks.get(key);
        if (yamlConfig == null) throw new Exception("Can not find " + key + " in sink configuration");

        Constructor<T> constructor = clazz.getDeclaredConstructor(Map.class);

        return constructor.newInstance(yamlConfig);
    }
}
