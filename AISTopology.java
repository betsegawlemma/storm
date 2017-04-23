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
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.*;

import java.util.*;
import java.io.*;


 public class AISTopology {

  public static class AISFormatBolt extends BaseRichBolt {

    OutputCollector _collector;

       List<ArrayList<String>> fLists = new ArrayList<ArrayList<String>>();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

       ArrayList arrayList =new ArrayList((ArrayList)tuple.getValue(0));
       String name = (arrayList.get(3)).toString();
       ArrayList<String> formatList = new ArrayList<String>();

        for(int i=0; i<4; i++){ 
 
           String in = (String)arrayList.get(i);
           if(i == 1 || i ==2)
               in = in.replace("\"","");
           formatList.add(in);
          
       } 
       formatList.add("\""+(((String)arrayList.get(4)+ "-"+(String)arrayList.get(5)+"-"+(String)arrayList.get(6)).replace("\"",""))+"\"");
       formatList.add("\""+(((String)arrayList.get(7)+ ":"+(String)arrayList.get(8)+":"+(String)arrayList.get(9)).replace("\"",""))+"\"");
       formatList.add("\""+(((String)arrayList.get(10)+ ":"+(String)arrayList.get(11)).replace("\"",""))+"\"");
       fLists.add(formatList);
       
      _collector.emit(tuple, new Values(formatList,name));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("formatList","name"));
    }


    @Override
    public void cleanup(){

       try{ 

          File file =new File("~/output/otherout.txt"); 
          if(!file.exists()){
            file.createNewFile();
          }
          BufferedWriter bw = new BufferedWriter(new FileWriter(file,true));
 
          bw.write("Name \t\t\t\t\t Speed \t\t Direction \t\t Date \t\t Time \t\t Position \t\t: \n");
     
        for(ArrayList f: fLists) {
      
             bw.write(f.get(3)+"\t\t\t\t\t"+f.get(1)+"\t\t"+f.get(2)+"\t\t"+f.get(4)+"\t\t"+f.get(5)+"\t\t"+f.get(6)+"\n");              
         }
         
         bw.close();
         
      }catch(Exception e){
      }
    }

  }

 public static class AISSelectNamedShipsBolt extends BaseRichBolt {

    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

       ArrayList namedShip =new ArrayList((ArrayList)tuple.getValue(0));
       String name = (namedShip.get(3)).toString();

      if(!name.isEmpty()){
            _collector.emit(tuple, new Values(namedShip));
       
      _collector.ack(tuple);

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("namedShip"));
    }



  }

 public static class AISShipCountBolt extends BaseRichBolt {

    List ships = new ArrayList();

    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

      String ship = (((ArrayList)tuple.getValue(0)).get(3)).toString(); 
      if(!ships.contains(ship)){
         ships.add(ship);

      }
      _collector.emit(tuple, new Values(ships.size()));

      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("totalShips"));
    }
    
    @Override
    public void cleanup(){
       try{ 
          File file =new File("~/output/shipcount.txt"); 
          if(!file.exists()){
            file.createNewFile();
          }	
	BufferedWriter bw = new BufferedWriter(new FileWriter(file,true));

          bw.write("Total number of ships: "+ships.size()+"\n");
	bw.close();

      }catch(Exception e){
      }
    }

  }

 public static class AISMesssagesPerShipBolt extends BaseRichBolt {

    OutputCollector _collector;
    Map<String, Integer> messagesPerShip = new HashMap<String, Integer>();

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
     
      String ship = (((ArrayList)tuple.getValue(0)).get(3)).toString();
      Integer count = messagesPerShip.get(ship);
     if(count == null)
       count = 0;
      count++;
      messagesPerShip.put(ship, count);
      _collector.emit(tuple, new Values(ship,count));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("ship","count"));
    }
   
    @Override
    public void cleanup(){
       
       try{ 
          File file =new File("~/output/messagecount.txt"); 
          if(!file.exists()){
            file.createNewFile();
          }	
	BufferedWriter bw = new BufferedWriter(new FileWriter(file,true));

      bw.write("Messages Per Ship\n");
      for(Map.Entry<String,Integer> e: messagesPerShip.entrySet()){
       bw.write(e.getKey()+" : "+e.getValue()+"\n");
      }
	bw.close();

      }catch(IOException e){
      }

    }


  }

 public static class AISMaxSpeedPerShipBolt extends BaseRichBolt {

    OutputCollector _collector;
    Map<String, Integer> maxSpeedPerShip = new HashMap<String, Integer>();
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
     
      ArrayList arrayList = (ArrayList)tuple.getValue(0);

      String ship = (arrayList.get(3)).toString();
      Integer speed = Integer.valueOf(((String)arrayList.get(2)).replace("\"",""));
      Integer maxSpeed = maxSpeedPerShip.get(ship);
      if(maxSpeed == null)
           maxSpeed = 0;
      maxSpeed = Math.max(speed,maxSpeed);
      maxSpeedPerShip.put(ship, maxSpeed);
      _collector.emit(tuple, new Values(ship,maxSpeed));
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields("ship","maxSpeed"));
    }

    @Override
    public void cleanup(){
        
	try{ 
          File file =new File("~/output/maxspeed.txt"); 
          if(!file.exists()){
            file.createNewFile();
          }	
	BufferedWriter bw = new BufferedWriter(new FileWriter(file,true));


      		bw.write("Maximum Speed of each Ship\n");
  
      for(Map.Entry<String,Integer> e: maxSpeedPerShip.entrySet()){
       		bw.write(e.getKey()+" : "+e.getValue()+"\n");
      }
           bw.close();
      } catch(IOException e) {
      }

    }


  }

  public static void main(String[] args) throws Exception {

    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("ais", new AISSpout(), 5);

    builder.setBolt("namedShips", new AISSelectNamedShipsBolt(), 3).shuffleGrouping("ais");
    builder.setBolt("format", new AISFormatBolt(), 3).shuffleGrouping("namedShips");
    builder.setBolt("shipCount", new AISShipCountBolt(), 3).fieldsGrouping("format", new Fields("name"));
    builder.setBolt("messagesPerShip", new AISMesssagesPerShipBolt(), 3).fieldsGrouping("format", new Fields("name"));
    builder.setBolt("maxSpeedPerShip", new AISMaxSpeedPerShipBolt(), 3).fieldsGrouping("format", new Fields("name"));

    Config conf = new Config();
    conf.put("aisfile",args[1]);
    conf.setDebug(true);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("test", conf, builder.createTopology());
      Utils.sleep(10000);
      cluster.killTopology("test");
      cluster.shutdown();
    }
  }
}
