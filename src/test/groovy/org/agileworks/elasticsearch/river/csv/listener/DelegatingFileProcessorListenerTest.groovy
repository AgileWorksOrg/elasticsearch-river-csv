package org.agileworks.elasticsearch.river.csv.listener

import spock.lang.Specification

/**
 * Created by vitek on 27/03/14.
 */
class DelegatingFileProcessorListenerTest extends Specification {

    FileProcessorListener listener1
    FileProcessorListener listener2

    DelegatingFileProcessorListener delegating

    void setup() {

        listener1 = GroovyMock(FileProcessorListener)
        listener2 = GroovyMock(FileProcessorListener)

        delegating = new DelegatingFileProcessorListener(listener1, listener2)
    }

    def "every listener should get notification once"() {

        when:

        delegating.onBeforeProcessingStart()

        then:

        1 * listener1.onBeforeProcessingStart()
        1 * listener2.onBeforeProcessingStart()
    }

    def "should return false as all listeners say no"() {

        when:

        boolean listening = delegating.listening()

        then:

        listener1.listening() >> false
        listener2.listening() >> false

        assert !listening
    }

    def "should return true if at least one listener says so"() {

        when:

        boolean listening = delegating.listening()

        then:

        listener1.listening() >> true
        listener2.listening() >> false

        assert listening
    }
}
