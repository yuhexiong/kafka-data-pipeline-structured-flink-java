package com.examples.parser;

import lombok.Getter;
import org.apache.commons.cli.*;

@Getter
public class CmdLineArgsParser {
    private final Options options;
    private String filePath;

    public CmdLineArgsParser(String[] args) {
        this.options = new Options();
        options.addOption("f", "file", true, "config file path");

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            // Parse the command line arguments
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
            System.exit(1);
        }

        if (cmd.hasOption("f")) {
            this.filePath = cmd.getOptionValue("f");
        } else {
            formatter.printHelp("should provide config file path", options);
            System.exit(1);
        }
    }
}
