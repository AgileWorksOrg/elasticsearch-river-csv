package org.agileworks.elasticsearch.river.csv.processrunner

import groovy.transform.CompileStatic

@CompileStatic
class WindowsProcessRunnerFactory implements ProcessRunnerFactory{

    @Override
    ProcessRunner create() {
        return new WindowsProcessRunner()
    }
}
