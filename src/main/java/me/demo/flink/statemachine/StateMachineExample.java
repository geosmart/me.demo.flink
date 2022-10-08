/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package me.demo.flink.statemachine;

import me.demo.flink.statemachine.dfa.State;
import me.demo.flink.statemachine.event.Alert;
import me.demo.flink.statemachine.event.Event;
import me.demo.flink.statemachine.generator.EventsGeneratorSource;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

/**\
 * me.demo.flink.statemachine.StateMachineExample --error-rate 0.1 --sleep 100
 * Main class of the state machine example. This class implements the streaming application that
 * receives the stream of events and evaluates a state machine (per originating address) to validate
 * that the events follow the state machine's rules.
 */
public class StateMachineExample {

    /**
     * Main entry point for the program.
     *
     * @param args The command line arguments.
     */
    public static void main(String[] args) throws Exception {
        System.out.println(
            "Usage with built-in data generator: StateMachineExample [--error-rate <probability-of-invalid-transition>] [--sleep <sleep-per-record-in-ms>]");
        System.out.println(
            "Usage with Kafka: StateMachineExample --kafka-topic <topic> [--brokers <brokers>]");
        System.out.println("Options for both the above setups: ");
        System.out.println("\t[--checkpoint-dir <filepath>]");
        System.out.println("\t[--async-checkpoints <true|false>]");
        System.out.println("\t[--incremental-checkpoints <true|false>]");
        System.out.println("\t[--output <filepath> OR null for stdout]");
        System.out.println();

        final ParameterTool params = ParameterTool.fromArgs(args);

        double errorRate = params.getDouble("error-rate", 0.0);
        int sleep = params.getInt("sleep", 1);

        System.out.printf(
            "Using standalone source with error rate %f and sleep delay %s millis\n",
            errorRate, sleep);
        System.out.println();

        final SourceFunction<Event> source = new EventsGeneratorSource(errorRate, sleep);

        // ---- main program ----

        // create the environment to create streams and configure execution
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000L);

        final String outputFile = params.get("output");

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<Event> events = env.addSource(source);

        DataStream<Alert> alerts =
            events
                // partition on the address to make sure equal addresses
                // end up in the same state machine flatMap function
                .keyBy(Event::sourceAddress)

                // the function that evaluates the state machine over the sequence of events
                .flatMap(new StateMachineMapper());

        // output the alerts to std-out
        if (outputFile == null) {
            alerts.print();
        } else {
            alerts.writeAsText(outputFile, FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        }

        // trigger program execution
        env.execute("State machine job");
    }

    // ------------------------------------------------------------------------

    /**
     * The function that maintains the per-IP-address state machines and verifies that the events
     * are consistent with the current state of the state machine. If the event is not consistent
     * with the current state, the function produces an alert.
     */
    @SuppressWarnings("serial")
    static class StateMachineMapper extends RichFlatMapFunction<Event, Alert> {

        /**
         * The state for the current key.
         */
        private ValueState<State> currentState;

        @Override
        public void open(Configuration conf) {
            // get access to the state object
            currentState =
                getRuntimeContext().getState(new ValueStateDescriptor<>("state", State.class));
        }

        @Override
        public void flatMap(Event evt, Collector<Alert> out) throws Exception {
            // get the current state for the key (source address)
            // if no state exists, yet, the state must be the state machine's initial state
            State state = currentState.value();
            if (state == null) {
                state = State.Initial;
            }

            // ask the state machine what state we should go to based on the given event
            State nextState = state.transition(evt.type());

            if (nextState == State.InvalidTransition) {
                // the current event resulted in an invalid transition
                // raise an alert!
                out.collect(new Alert(evt.sourceAddress(), state, evt.type()));
            } else if (nextState.isTerminal()) {
                // we reached a terminal state, clean up the current state
                currentState.clear();
            } else {
                // remember the new state
                currentState.update(nextState);
            }
        }
    }
}
