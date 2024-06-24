package com.examples.parser;

import org.apache.flink.shaded.jackson2.org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.util.Map;

public class YamlParser {
    final public String jobName;
    final private Map<String, Map<String, Object>> sources;
    final private Map<String, Map<String, Object>> sinks;

    public YamlParser(InputStream in) throws Exception {
        Map<String, Object> setting = new Yaml().load(in);
        if (setting.get("jobName") == null) {
            throw new Exception("Should provide jobName");
        }

        jobName = (String) setting.get("jobName");
        this.sources = (Map<String, Map<String, Object>>) setting.get("source");
        this.sinks = (Map<String, Map<String, Object>>) setting.get("sink");
    }

    public Map<String, Object> getSourceConfig(String key) throws Exception {
        // check sources
        if (sources == null) throw new Exception("Can not find source configuration");

        // check source setting
        Map<String, Object> setting = sources.get(key);
        if (setting == null) throw new Exception("Can not find " + key + " in source configuration");

        return setting;
    }

    public Map<String, Object> getSinkConfig(String key) throws Exception {
        // check sinks
        if (sinks == null) throw new Exception("Can not find sink configuration");

        // check sink setting
        Map<String, Object> setting = sinks.get(key);
        if (setting == null) throw new Exception("Can not find " + key + " in sink configuration");

        return setting;
    }
}
