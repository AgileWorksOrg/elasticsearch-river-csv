package org.agileworks.elasticsearch.river.csv.processrunner

import groovy.transform.CompileStatic

@CompileStatic
class GeneralProcessRunner implements ProcessRunner{

    String runScript(File file, Object... args) {

        if (file && file.exists()) {                

            def process = [file.absolutePath, args.join(" ")].execute()

            return process.text
        }
    }
}
