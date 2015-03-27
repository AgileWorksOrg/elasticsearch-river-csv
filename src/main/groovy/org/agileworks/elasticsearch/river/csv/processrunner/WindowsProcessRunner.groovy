package org.agileworks.elasticsearch.river.csv.processrunner

import groovy.transform.CompileStatic

@CompileStatic
class WindowsProcessRunner implements ProcessRunner{

    String runScript(File file, Object... args) {

        if (file && file.exists()) {
                
            def process

            String fileName = file.name.toLowerCase()

            if (fileName.endsWith('.sh') ) {

                process = ["sh", file.absolutePath, args.join(" ")].execute()                    

            } else if (fileName.endsWith('.ps1')) {

                process = ["PowerShell", "-NoLogo", "-NoProfile", "-NonInteractive", "-File", file.absolutePath, args.join(" ")].execute()                    

            } else if (fileName.endsWith('.wsf') || fileName.endsWith('.wsh') || fileName.endsWith('.vbs') || fileName.endsWith('.js')) {

                process = ["CScript", "//NoLogo", file.absolutePath, args.join(" ")].execute()                    

            } else {

                process = [file.absolutePath, args.join(" ")].execute()

            }

            return process.text
        }
    }
}
