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
package net.onrc.openvirtex.core.io;

import java.util.concurrent.ThreadPoolExecutor;

import net.onrc.openvirtex.core.OpenVirteXController;
import net.onrc.openvirtex.elements.datapath.OVXSwitch;
import net.onrc.openvirtex.elements.network.PhysicalNetwork;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.handler.execution.ExecutionHandler;
import org.jboss.netty.handler.timeout.IdleStateHandler;
import org.jboss.netty.handler.timeout.ReadTimeoutHandler;
import org.jboss.netty.handler.traffic.ChannelTrafficShapingHandler;
import org.jboss.netty.handler.traffic.GlobalChannelTrafficShapingHandler;
import org.jboss.netty.handler.traffic.GlobalChannelTrafficCounter;
import org.jboss.netty.handler.traffic.TrafficCounter;

import java.util.concurrent.atomic.AtomicLong;

public class ClientChannelPipeline extends OpenflowChannelPipeline {
	Logger log = LogManager.getLogger(ClientChannelPipeline.class.getName());
    private ClientBootstrap bootstrap = null;
    private OVXSwitch sw = null;
    private final ChannelGroup cg;
    final int M = 1024*1024; //M = 1MB = 1024KB = 1024 * 1024 B
    final int K = 1024; //K = 1024B
    private long lastReads;
    private long lastWrites;
   
    
    private final AtomicLong lastChacked = new AtomicLong();
    private final AtomicLong currentReads = new AtomicLong();
    private final AtomicLong currentWrites = new AtomicLong();
    

    public ClientChannelPipeline(
            final OpenVirteXController openVirteXController,
            final ChannelGroup cg, final ThreadPoolExecutor pipelineExecutor,
            final ClientBootstrap bootstrap, final OVXSwitch sw) {
        super();
        this.ctrl = openVirteXController;
        this.pipelineExecutor = pipelineExecutor;
        this.timer = PhysicalNetwork.getTimer();
        this.idleHandler = new IdleStateHandler(this.timer, 20, 25, 0);
        this.readTimeoutHandler = new ReadTimeoutHandler(this.timer, 30);
        this.controllerHandler  = new ControllerChannelHandler(ctrl,sw);
        //this.clientChannelShapingHandler = new ChannelTrafficShapingHandler(timer,50*K,50*K);
        //this.globalChannelTrafficShapingHandler = new GlobalChannelTrafficShapingHandler(this.timer,0,0,5000,5000);
        this.globalChannelTrafficShapingHandler = new GlobalChannelTrafficShapingHandler(this.timer,316*K,316*K,4000,4000);
        //this.globalChannelTrafficShapingHandler = new GlobalChannelTrafficShapingHandler(this.timer,1400*M,1400*M,100*M,100*M); //Defualt
        //this.trafficCounter = new TrafficCounter(this.clientChannelShapingHandler,this.timer,sw.getSwitchName(),15);
        //this.trafficCounter = this.clientChannelShapingHandler.getTrafficCounter();
        this.globalTrafficCounter = new GlobalChannelTrafficCounter(this.globalChannelTrafficShapingHandler,this.timer,"GlobalTC",15);
        this.bootstrap = bootstrap;
        this.sw = sw;
        this.cg = cg;
        
        this.log.info("@@@@@@@@@@@@@@@@@@::::::::::ClientChannelPipeline created::::::::::::::::%%%%%%%%%%%%%%%%%%%");
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception {
    	//final int M = 1024 * 1024; //M = 1MB 
    	//final ControllerChannelHandler handler = new ControllerChannelHandler(this.ctrl, this.sw);
    	
    	
    	//ChannelTrafficShapingHandler channelTrafficShapingHandler = new ChannelTrafficShapingHandler(timer,5*M, M/2);
    	//TrafficCounter trafficCounter = this.clientChannelShapingHandler.getTrafficCounter();
    	//String readThroughput = trafficCounter.toString();
    	 

         final ChannelPipeline pipeline = Channels.pipeline();
         pipeline.addLast("reconnect", new ReconnectHandler(this.sw,
                 this.bootstrap, this.timer, 15, this.cg));
         pipeline.addLast("ofmessagedecoder", new OVXMessageDecoder());
         pipeline.addLast("ofmessageencoder", new OVXMessageEncoder());
         pipeline.addLast("idle", this.idleHandler);
         pipeline.addLast("timeout", this.readTimeoutHandler);
         pipeline.addLast("handshaketimeout", new HandshakeTimeoutHandler(
        		 this.controllerHandler, this.timer, 15));
         //Channel bandwidth
         //pipeline.addLast("channel_Traffic_ShapingHandler", this.clientChannelShapingHandler);
         pipeline.addLast("globalchanneltrafficShapingHandler", this.globalChannelTrafficShapingHandler);
         pipeline.addLast("pipelineExecutor", new ExecutionHandler(
                 this.pipelineExecutor));
         pipeline.addLast("handler", this.controllerHandler);
         //pipeline.addLast("globalchanneltrafficShapingHandler", this.globalChannelTrafficShapingHandler);
         //pipeline.replace(oldHandler, newName, newHandler);
         //TrafficCounter trafficCount = makeTrafficCounter();
        
         
         
         this.log.info("@@@@@@@@@@@@@@@@@@::::::::::ClientChannelPipeline.getPipeline:: created::::::::::::::::%%%%%%%%%%%%%%%%%%%");
         //pipeline.getContext("channel_Traffic_ShpingHandler");
         return pipeline;
    }
    public TrafficCounter makeTrafficCounter(){
    	//this.trafficCounter.start();
    	this.log.info("info : {},{} /  check channel`s Traffic Counter: {} // check interval: {}",this.trafficCounter.getName(),this.sw.getTenantId(),this.trafficCounter.toString(), this.trafficCounter.getCheckInterval());
    	return this.trafficCounter;
    }
    
    public void doAccounting(TrafficCounter counter) {
    	//TrafficCounter counter = this.clientChannelShapingHandler.getTrafficCounter();
        long currentTime = System.currentTimeMillis();
        long interval = currentTime - lastChacked.getAndSet(currentTime);
        if (interval == 0) {
            return;
        }
        this.lastReads = this.currentReads.getAndSet(0L);
        this.lastWrites = this.currentWrites.getAndSet(0L);
 
        long readsPerSec = (lastReads / interval) * 1000;
        long writesPerSec = (lastWrites / interval) * 1000;
        //metrics.setLastReads(readsPerSec);
        //metrics.setLastWrites(writesPerSec);
 
        //TrafficCounter traffic = this.makeTrafficCounter();
        long readThroughput = counter.getLastReadThroughput();
        long writeThroughput = counter.getLastWriteThroughput();
        //metrics.setReadThroughput(readThroughput);
        //metrics.setWriteThroughput(writeThroughput);
        log.info("Reads per Sec: {}", readsPerSec);
        log.info("Writes per Sec: {}", writesPerSec);
        log.info("Read Throughput: {}", readThroughput);
        log.info("Write Throughput: {}", writeThroughput);

        /*
        if (log.isInfoEnabled()) {
            if (lastReads > 0 || lastWrites > 0) {
                log.info(toString());
            }
        }
        */
    }
    @Override
    public String toString() {
        TrafficCounter traffic = this.clientChannelShapingHandler.getTrafficCounter();
        final StringBuilder buf = new StringBuilder(512);
        long readThroughput = traffic.getLastReadThroughput();
        buf.append("Read Throughput: ").append(readThroughput / 1024L).append(" KB/sec, ");
        //buf.append(lastReads).append(" msg/sec\n");
        long writeThroughput = traffic.getLastWriteThroughput();
        buf.append("Write Throughput: ").append(writeThroughput / 1024).append(" KB/sec, ");
        //buf.append(lastWrites).append(" msg/sec");
        long Read = traffic.getLastReadBytes();
        long Write = traffic.getLastWrittenBytes();
        //read/write bytes
        buf.append("Read Bytes: ").append(Read).append(" Bytes");
        buf.append("Write Bytes: ").append(Write).append(" Bytes");
        return buf.toString();
    }
    


}
