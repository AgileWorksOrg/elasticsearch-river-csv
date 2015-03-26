package org.agileworks.elasticsearch.river.csv.processrunner

public interface ProcessRunner {

    String runScript(File file, Object... args)
}
