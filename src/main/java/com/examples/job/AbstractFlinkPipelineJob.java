package com.examples.job;

import com.examples.parser.CmdLineArgsParser;
import com.examples.parser.YamlParser;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.io.*;
import java.net.URL;

public abstract class AbstractFlinkPipelineJob {
    protected final StreamExecutionEnvironment env;
    protected String jobName;
    protected final CmdLineArgsParser cmdLineArgs;

    public AbstractFlinkPipelineJob(String[] args) {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.env.enableCheckpointing(10000);
        this.env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        this.cmdLineArgs = new CmdLineArgsParser(args);
    }

    public YamlParser loadSetting() throws IOException {
        if (cmdLineArgs.getFilePath() != null) {
            String path = cmdLineArgs.getFilePath();
            InputStream inputStream = null;

            try {
                if (path.startsWith("http://") || path.startsWith("https://")) {
                    URL url = new URL(path);
                    inputStream = url.openStream();
                } else {
                    File file = new File(path);
                    inputStream = new FileInputStream(file);
                }

                YamlParser yamlSetting = new YamlParser(inputStream);
                this.jobName = yamlSetting.jobName;
                return yamlSetting;
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (inputStream != null) {
                    try {
                        inputStream.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }

        }
        return null;
    }

    protected void run() throws Exception {
        this.env.execute(this.jobName);
    }
}
