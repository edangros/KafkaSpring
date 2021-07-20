package com.inspien.kafka.connect;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;

import com.inspien.kafka.connect.error.NoTaskException;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.runtime.ConnectorConfig;
import org.apache.kafka.connect.source.SourceTask;

import lombok.extern.slf4j.Slf4j;


/**
 * The load balancer for Connect {@link SourceTask}s.
 * TaskLoadBalancer subscribes task's load change event and finds appropriate task to take the load.
 */
@Slf4j
public class TaskLoadBalancer implements ILoadBalancer<RESTInputSourceTask>, Closeable{


    private HashMap<RESTInputSourceTask, Integer> tasks;
    private String connectionId;
    private RESTInputSourceTask targetTask;

    /**
     * Generate TaskLoadBalancer with configuration. 
     * @param config configuration of the load balancer. Connector's config could be directly passed.
     */
    public TaskLoadBalancer(AbstractConfig config){
        this.connectionId = config.getString(ConnectorConfig.NAME_CONFIG);
        this.tasks = new HashMap<>();
        log.info("Load Balancer for {} is activated",this.connectionId);
        RESTContextManager.getInstance().registerLB(this.connectionId, this);
    }

    
    @Override
    public void register(RESTInputSourceTask object) {
        if (tasks.keySet().contains(object)){
            log.warn("Task {} is already registered in loadbalancer. Just update loadbalancer", object.hashCode());
        }
        this.tasks.put(object, object.loadScore());
        if ((this.targetTask == null) || (this.tasks.get(this.targetTask) > object.loadScore())) this.targetTask = object;
    }

    @Override
    public void deregister(RESTInputSourceTask member) {
        if (!this.tasks.keySet().contains(member))
            log.warn("Loadbalancer requested unregister of unregistered item {}", member.hashCode());
        this.tasks.remove(member);
        log.trace("Task {} is unloaded from loadbalancer", member.hashCode());
        //readdress appropriate member
        if (this.targetTask.equals(member)){
            this.readdress();
        }
    }

    /**
     * accumulate loads of each task, and find the task with lowest load
     */
    private void readdress(){
        //if no item left, set to null
        if(this.tasks.size() <= 0) this.targetTask = null;
        int lowest = Integer.MAX_VALUE;
        for (RESTInputSourceTask task : new ArrayList<RESTInputSourceTask>(tasks.keySet())){
            if (this.tasks.get(task) <= lowest){
                lowest = this.tasks.get(task);
                this.targetTask = task;
            }
        }
    }

    @Override
    public RESTInputSourceTask getAppropriate() {
        if (this.targetTask == null){
            throw new NoTaskException("no task is registered in loadbalancer. Try wait tasks to ready");
        }
        return this.targetTask;
    }

    @Override
    public void updatescore(RESTInputSourceTask member, int newscore) {
        if (!this.tasks.keySet().contains(member)){
            log.error("loadbalancer does not have member task {} but member update is requested, which has no effect.", member.hashCode());
            return;
        }
        this.tasks.put(member, newscore);
        if (this.tasks.get(this.targetTask) > newscore) this.targetTask = member;
    }


    @Override
    public void close() {
        log.info("loadbalancer for {} is closed",this.connectionId);
    }    
}
