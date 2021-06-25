/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.task.ShellBolt;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.starter.spout.RandomSentenceSpout;

import java.util.HashMap;
import java.util.Map;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */

// To submit this topology, use
// storm jar /usr/hdp/current/storm-client/contrib/storm-starter/storm-starter-topologies-*.jar org.apache.storm.starter.WordCountTopology wordCount
// Then Storm will start running this job forever, and the job can be monitored at http://127.0.0.1:8744

public class WordCountTopology {

  // This topology listens to a spout of incoming sentences, and count word frequencies.

  public static class SplitSentence extends ShellBolt implements IRichBolt {

    // constructor
    public SplitSentence() {
      // Supposedly splitsentence.py just calls the split() method on python strings.
      super("python", "splitsentence.py");
    }

    @Override // Here we specify a bolt output. This will output the words by calling the splitted sentences.
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
      return null;
    }
  }

  // WordCount is another bolt. For each word it will update the hashmap.
  public static class WordCount extends BaseBasicBolt {
    Map<String, Integer> counts = new HashMap<String, Integer>();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
      String word = tuple.getString(0);
      Integer count = counts.get(word);
      if (count == null)
        count = 0;
      count++;
      counts.put(word, count);
      // Emit the final results, complying to the format declared in declareOutputFields
      collector.emit(new Values(word, count));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("word", "count"));
    }
  }

  public static void main(String[] args) throws Exception {

    // This is where we start to build a topology.
    TopologyBuilder builder = new TopologyBuilder();

    // RandomSentenceSpout is defined in storm starter kit. 
    // '5' is the parallelismHint: the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a process somewhere around the cluster.
    // Same with setBolt()
    builder.setSpout("spout", new RandomSentenceSpout(), 5);

    // A bolt defined by SplitSentence class
    // shuffleGrouping: Tuples are randomly distributed across the bolt’s tasks in a way such that each bolt is guaranteed to get an equal number of tuples.
    builder.setBolt("split", new SplitSentence(), 8).shuffleGrouping("spout");
    
    // A chained bolt to deliver the word count
    // fieldsGrouping: The stream is partitioned by the fields specified in the grouping.
    builder.setBolt("count", new WordCount(), 12).fieldsGrouping("split", new Fields("word"));

    // As we aren't dumping the data into a specific store, they all go to the logs at /usr/hdp/current/storm-client/logs/workers-artifacts

    Config conf = new Config();
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {
      conf.setMaxTaskParallelism(3);

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("word-count", conf, builder.createTopology());

      Thread.sleep(10000);

      cluster.shutdown();
    }
  }
}
