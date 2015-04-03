package org.agileworks.elasticsearch.river.csv.processrunner

import groovy.transform.CompileStatic

@CompileStatic
class ProcessRunnerFactory {

    private static final ProcessRunnerFactory INSTANCE = new ProcessRunnerFactory()
    public static ProcessRunnerFactory getInstance() { 
        return INSTANCE 
    }

    ProcessRunner getRunner() {
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            return new WindowsProcessRunner()
        }

        return new GeneralProcessRunner()
    }
}
