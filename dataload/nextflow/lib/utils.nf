// Utility functions for GREBI Nextflow pipeline

import groovy.json.JsonSlurper

def parseJson(json) {
    return new JsonSlurper().parseText(json)
}

def getStdinCommand(ingest, filename) {
    if (ingest.stdin == false) {
        return ""
    }
    def f = new File(filename.toString()).getName()
    if (f.endsWith(".gz")) {
        return "zcat ${f} |"
    } else if (f.endsWith(".xz")) {
        return "xzcat ${f} |"
    } else {
        return "cat ${f} |"
    }
}

def buildAddEquivGroupArgs(equivGroups) {
    def res = ""
    equivGroups.each { arg -> res += "--add-group ${arg.iterator().join(",")} " }
    return res
}

def buildMergeArgs(assigned) {
    def res = ""
    assigned.each { a ->
        res += "${a[0]}:${a[1]} "
    }
    return res
}

def basename(filename) {
    return new File(filename).name
}
