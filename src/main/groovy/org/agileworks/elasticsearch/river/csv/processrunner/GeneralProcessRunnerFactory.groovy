package org.agileworks.elasticsearch.river.csv.processrunner

import groovy.transform.CompileStatic

@CompileStatic
class GeneralProcessRunnerFactory implements ProcessRunnerFactory{

    @Override
    ProcessRunner create() {
        return new GeneralProcessRunner()
    }
}
