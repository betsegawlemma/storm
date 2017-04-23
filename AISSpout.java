package org.apache.storm.starter;

import org.apache.storm.Config;
import org.apache.storm.topology.OutputFieldsDeclarer;
import java.util.*;
import java.io.*;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import java.util.HashMap;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class AISSpout extends BaseRichSpout {
    public static Logger LOG = LoggerFactory.getLogger(AISSpout.class);
    boolean _isDistributed;
    SpoutOutputCollector _collector;
    boolean completed = false;
    Scanner fileInput;
    String filename;
    public AISSpout() {
        this(true);
    }

    public AISSpout(boolean isDistributed) {
        _isDistributed = isDistributed;
    }
        
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
  
        _collector = collector;

     try{
	fileInput = new Scanner(new File(conf.get("aisfile").toString()));
     }catch(Exception e){
        throw new RuntimeException(e);
     }
              
     }
    
    public void close() {
         
         fileInput.close();
        
    }
        
    public void nextTuple() {

     Utils.sleep(100); 
     try{
     if(fileInput.hasNextLine()){
          
         String[] oneLine = fileInput.nextLine().split(",");

      
      final ArrayList<String> ais = new ArrayList(Arrays.asList(oneLine));
        _collector.emit(new Values(ais));

    }
    }catch(Exception e){
       throw new RuntimeException(e.getMessage());
   }
 
    }

    public void ack(Object msgId) {

    }

    public void fail(Object msgId) {
        
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("ais"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        if(!_isDistributed) {
            Map<String, Object> ret = new HashMap<String, Object>();
            ret.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
            return ret;
        } else {
            return null;
        }
    }    
}

