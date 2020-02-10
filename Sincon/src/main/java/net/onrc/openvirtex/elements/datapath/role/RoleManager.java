/*
 * ******************************************************************************
 *  Copyright 2019 Korea University & Open Networking Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  ******************************************************************************
 *  Developed by Libera team, Operating Systems Lab of Korea University
 *  ******************************************************************************
 */
package net.onrc.openvirtex.elements.datapath.role;

import java.text.DateFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import net.onrc.openvirtex.core.io.ClientChannelPipeline;
import net.onrc.openvirtex.elements.network.PhysicalNetwork;


import java.util.concurrent.atomic.AtomicReference;

import net.onrc.openvirtex.exceptions.UnknownRoleException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.channel.Channel;
import org.projectfloodlight.openflow.protocol.OFControllerRole;
import org.projectfloodlight.openflow.protocol.OFMessage;
import org.jboss.netty.handler.traffic.*;
import org.jboss.netty.channel.ChannelPipeline;

public class RoleManager {

    private static Logger log = LogManager.getLogger(RoleManager.class
            .getName());
    private HashMap<Channel, Role> state;
    private final AtomicReference<HashMap<Channel, Role>> currentState;
    private Channel currentMaster;

    
    public enum Role {
        EQUAL,
        MASTER,
        SLAVE,
        NOCHANGE;
    }
    public static long M = 1024 * 1024;
    public static long K = 1024;
    public static int handler_num = 0;
    public static long adaptiveLimit = 158*K;  //default = 700
    public static long defalutAdpativeLimit = 158*K;
    public static long debtThroughput = 0;
    public static boolean initalCheck = false;
    
    //Sincon integer;
    public static long[][] cumulative_throughputs = new long [6][30];
    public static int[][] message_counts = new int [6][30];
    public static long[][] average_throughput = new long [6][30];
    public static int[][] initial_Check = new int[6][30];
    public static int initalCheckAll = 0; 
    //public static long adaptiveLimit = 700*M;  //default = 700
    //public static long defalutAdpativeLimit = 700*M;

    public RoleManager() {
        this.state = new HashMap<Channel, Role>();
        this.currentState = new AtomicReference<HashMap<Channel, Role>>(state);
    }
    private HashMap<Channel, Role> getState() {
        return new HashMap<>(this.currentState.get());
    }

    private void setState() {
        this.currentState.set(this.state);
    }

    public synchronized void addController(Channel chan) {
        if (chan == null) {
            return;
        }
        this.state = getState();
        this.state.put(chan, Role.EQUAL);
        setState();
    }

    public synchronized void setRole(Channel channel, Role role)
            throws IllegalArgumentException, UnknownRoleException {
        if (!this.currentState.get().containsKey(channel)) {
            throw new IllegalArgumentException("Unknown controller "
                    + channel.getRemoteAddress());
        }
        this.state = getState();
        log.info("Setting controller {} to role {}",
                channel.getRemoteAddress(), role);
        switch (role) {
            case MASTER:
                if (channel == currentMaster) {
                    this.state.put(channel, Role.MASTER);
                    break;
                }
                this.state.put(currentMaster, Role.SLAVE);
                this.state.put(channel, Role.MASTER);
                this.currentMaster = channel;
                break;
            case SLAVE:
                if (channel == currentMaster) {
                    this.state.put(channel, Role.SLAVE);
                    currentMaster = null;
                    break;
                }
                this.state.put(channel, Role.SLAVE);
                break;
            case EQUAL:
                if (channel == currentMaster) {
                    this.state.put(channel, Role.EQUAL);
                    this.currentMaster = null;
                    break;
                }
                this.state.put(channel, Role.EQUAL);
                break;
            case NOCHANGE:
                // do nothing
                break;
            default:
                throw new UnknownRoleException("Unkown role : " + role);

        }
        setState();

    }

    public boolean canSend(Channel channel, OFMessage m) {
        Role r = this.currentState.get().get(channel);
        if (r == Role.MASTER || r == Role.EQUAL) {
            return true;
        }
        switch (m.getType()) {
            case GET_CONFIG_REQUEST:
            case QUEUE_GET_CONFIG_REQUEST:
            case PORT_STATUS:
            case STATS_REQUEST:
                return true;
            default:
                return false;
        }
    }

    public boolean canReceive(Channel channel, OFMessage m) {
        Role r = this.currentState.get().get(channel);
//        log.info(r.toString());

        if (r == Role.MASTER || r == Role.EQUAL) {
//            log.info("r == Role.MASTER || r == Role.EQUAL");
            return true;
        }else{
//            log.info(" not r == Role.MASTER || r == Role.EQUAL");
        }

        switch (m.getType()) {
            case GET_CONFIG_REPLY:
            case QUEUE_GET_CONFIG_REPLY:
            case PORT_STATUS:
            case STATS_REPLY:
                return true;
            default:
                return false;
        }
    }

    public Role getRole(Channel channel) {
        return this.currentState.get().get(channel);
    }

    public long checkAndSend(Channel c, OFMessage m, int tId, int swDec) {
//        log.info("checkAndSend");
        long currentThroughput = -1;
        long averageThroughput = 0;
    	long cumulativesValue = 0;
    	if (canReceive(c, m)) {
            if (c != null && c.isOpen()) {
                c.write(Collections.singletonList(m));
                GlobalChannelTrafficShapingHandler gct = (GlobalChannelTrafficShapingHandler) c.getPipeline().get("globalchanneltrafficShapingHandler");
                currentThroughput = sinconMonitor(gct,tId,swDec);
                // String result
                //log.info("sinconString: {}",sinconString(countResult,c)); 
            	if(currentThroughput > 0){
            		cumulative_throughputs[tId][swDec]+=currentThroughput;
                    message_counts[tId][swDec] += 1;
                    average_throughput[tId][swDec] = cumulative_throughputs[tId][swDec] / message_counts[tId][swDec];
            	}else if(currentThroughput == -100){ // intialize
            		// case of adaptive throguhput = 0 -> initailize
            		if(initalCheckAll == 0){
            			cumulativesValue = 0;
            			for(int i=1;i<5;i++){
        		    		for(int j=1;j<11;j++){
        		    			cumulativesValue += average_throughput[i][j];
        		    		}
        		    	}
            		}
            		if(initalCheckAll != 40){
            			if (initial_Check[tId][swDec] == 0){
            				initalCheckAll++;
            				log.info("({}/{})SINCON INTIALIZE---- :{}/40",tId,swDec,initalCheckAll);
            				averageThroughput = average_throughput[tId][swDec];
            				initial_Check[tId][swDec]++;
            				boolean finishInitial = false;
            				finishInitial = sinconInitialize(cumulativesValue, initalCheckAll ,averageThroughput, c);
            				if(finishInitial == true){
                    			log.info("SINCON-- Start Initialize throughput!!");
                    			cumulative_throughputs = new long[5][30];
                    			message_counts = new int[5][30];
                    			average_throughput = new long[5][30];
                    			initial_Check = new int[6][30];
                    			initalCheckAll = 0;
                    			
                    		}
            			}
            		}else{
            			initalCheckAll = 0;
            		}
            	}else if(currentThroughput == -1){
            		log.error("send Msg error: No output value for Current Throughput");
            	}
                
            }
        }
    	return currentThroughput;
    }
    public long sinconMonitor(GlobalChannelTrafficShapingHandler gct, int tId, int swDec){
    	if(initalCheck == true){
    		adaptiveLimit -= debtThroughput;
    		
    		if(adaptiveLimit < 0){
    			log.warn("No more adpatable throughput, left debt = {} ", adaptiveLimit);
    			adaptiveLimit = 0;
    		}
    		debtThroughput = 0;
    		initalCheck = false;
    	}
    	long readLimit = gct.getReadChannelLimit(); //4000
    	long writeLimit = gct.getWriteChannelLimit(); //4000
    	long standardReadThroughut = (long) (readLimit * 0.9); //3600 
    	//long standardWriteThroughut = (long) (writeLimit * 0.9); 
    	long addReadThroughput = (long) (readLimit * 0.1);  //400
    	//long addWriteThroughput = (long) (writeLimit * 0.1);
    	
    	long futureReadThroughput = readLimit + addReadThroughput;  //bonus +10% = 4400
    	long futureWriteThroughput = writeLimit + addReadThroughput; //bonus +10% = 4400
    	
    	if(gct.getTrafficCounter().getLastReadThroughput() > standardReadThroughut ){
         	
         	if(gct.getTrafficCounter().getLastReadThroughput()>gct.getReadChannelLimit()){
         	   //over throughput limit 
         		log.info("Over Throughput>>");
         	}
         	
         	gct.setReadChannelLimit(futureReadThroughput);
         	gct.setWriteChannelLimit(futureWriteThroughput);
         	adaptiveLimit -= addReadThroughput;
         	         	
         }
    	
    	if(adaptiveLimit < 0){
     		debtThroughput += Math.abs(addReadThroughput);
     		adaptiveLimit=0;
     	}
     	if(adaptiveLimit ==0){ // adaptiveLimit go zero -> initailaize to average throughput average
    		return -100;
    	}
     	
    	log.info("SinconMonitor:TId({})Sw({}):: CurrnetThroughput> {}/ Limit> {}/ Adapt> {} / debt> {}",tId,swDec,gct.getTrafficCounter().getLastReadThroughput(),gct.getReadChannelLimit(),adaptiveLimit, debtThroughput);
    	return gct.getTrafficCounter().getLastReadThroughput();
    }
    public String sinconString(GlobalChannelTrafficShapingHandler nct){
    	String montiorString = " "; 
    	montiorString += 
         			//" messageType : "+ m.getType()+ 
         			//" // Traffic Count`s current : " +ct.getTrafficCounter().toString() +
         			//" // Last Data size: " +nct.getTrafficCounter().getLastReadBytes()+"/"+nct.getTrafficCounter().getLastWrittenBytes()+
         			//" // Last Time: "+gct.getTrafficCounter().getLastTime()+
         			" // Last throughput: " + nct.getTrafficCounter().getLastReadThroughput() + "/"+ nct.getTrafficCounter().getLastWriteThroughput()+
         			//" // inforimation: " + nct.getTrafficCounter().getCheckInterval() +
         			" // current Chanel Limit:  " + nct.getReadChannelLimit() + "/" + nct.getWriteChannelLimit()+
         			//" //cumulative:  " + gct.getTrafficCounter().getCumulativeReadBytes() + " / "+ gct.getTrafficCounter().getCumulativeWrittenBytes();
         			//" //cumTime: " + ct.getTrafficCounter().getLastCumulativeTime()+
         			" //queue size: "+ nct.queuesSize();
        
    	return montiorString;
    }
    public static boolean sinconInitialize(long cumulatives, int initalCheck_All, long average, Channel c){
        GlobalChannelTrafficShapingHandler gct = (GlobalChannelTrafficShapingHandler) c.getPipeline().get("globalchanneltrafficShapingHandler");
    	gct.setReadChannelLimit(average);
    	gct.setWriteChannelLimit(average);
    	if(initalCheck_All == 40){
    		adaptiveLimit = defalutAdpativeLimit * 2 - cumulatives;
        	initalCheck =true;
        	//c.getPipeline().removeLast();
        	//c.getPipeline().addLast(handler_name,  new GlobalChannelTrafficShapingHandler(PhysicalNetwork.getTimer(), 316*K,316*K,averageValue,averageValue));
        	log.info("SINCON-- Initialize Success!!!");
        	return true;
    	}
    	return false;
    	
    }

     public long sendMsg(OFMessage msg, Channel c, int tId, int swDec) {
//        log.info("sendMsg");
    	long throughput = -1;
        if (c != null) {
        	throughput =  checkAndSend(c, msg,tId, swDec);
        } else {
            final Map<Channel, Role> readOnly = Collections
                    .unmodifiableMap(this.currentState.get());
            for (Channel chan : readOnly.keySet()) {
                if (chan == null) {
                    continue;
                }
                throughput = checkAndSend(chan, msg, tId, swDec);
            }
        }
        return throughput;
    }
 /*    String sendMsg(OFMessage msg, Channel c) {
//  log.info("sendMsg");

    	 if (c != null) {
      return checkAndSend(c, msg);
  		} 
  else {
      final Map<Channel, Role> readOnly = Collections
              .unmodifiableMap(this.currentState.get());
      for (Channel chan : readOnly.keySet()) {
          if (chan == null) {
              continue;
          }
          return checkAndSend(chan, msg);
      		}
  		}
     }*/

    public synchronized void removeChannel(Channel channel) {
        this.state = getState();
        this.state.remove(channel);
        setState();
    }

    public synchronized void shutDown() {
        this.state = getState();
        for (Channel c : state.keySet()) {
            if (c != null && c.isConnected()) {
                c.close();
            }
        }
        state.clear();
        setState();
    }

    @Override
    public String toString() {
        return this.currentState.get().toString();
    }
}
