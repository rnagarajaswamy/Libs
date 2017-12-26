/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package xpo.kestone.samza.api;

import org.apache.samza.application.StreamApplication;
import org.apache.samza.config.Config;
import org.apache.samza.operators.MessageStream;
import org.apache.samza.operators.OutputStream;
import org.apache.samza.operators.StreamGraph;
import org.apache.samza.operators.windows.WindowPane;
import org.apache.samza.operators.windows.Windows;
import org.apache.samza.operators.functions.JoinFunction;
import org.apache.samza.operators.functions.FoldLeftFunction;
import org.apache.samza.metrics.Counter;
import org.apache.samza.storage.kv.KeyValueStore;
import org.apache.samza.task.TaskContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.google.common.collect.ImmutableList;

import java.time.Duration;
import java.util.Collection;
import java.util.function.Function;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TopicMerger implements StreamApplication{
    private static final Logger LOG = LoggerFactory.getLogger(TopicMerger.class);
    private static final String INPUT_TOPIC_001 = "input001";
    private static final String INPUT_TOPIC_002 = "input002";

    private static final String OUTPUT_TOPIC = "output001";
    @Override
    public void init(StreamGraph graph, Config config) {

        MessageStream<String> iStream001 = graph.<String, String, String>getInputStream(INPUT_TOPIC_001, (k, v) -> v);
        MessageStream<String> iStream002 = graph.<String, String, String>getInputStream(INPUT_TOPIC_002, (k, v) -> v);

        OutputStream<String, String, String> oStream = graph.getOutputStream(OUTPUT_TOPIC, m -> "", m -> m);

        MessageStream<String> streamEvents = MessageStream.mergeAll(ImmutableList.of(iStream001, iStream002));

        streamEvents.sendTo(oStream);
    }

}
