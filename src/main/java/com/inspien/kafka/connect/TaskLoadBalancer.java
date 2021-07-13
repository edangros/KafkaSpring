package com.inspien.kafka.connect;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.connect.source.SourceTask;

import lombok.extern.slf4j.Slf4j;


/**
 * The load balancer for Connect {@link SourceTask}s.
 * TaskLoadBalancer subscribes task's load change event and finds appropriate task to take the load.
 */
@Slf4j
public class TaskLoadBalancer implements ILoadBalancer<RESTInputSourceTask>{


    private HashMap<RESTInputSourceTask, Integer> tasks;
    private String name;
    private RESTInputSourceTask appropriateItem;

    public TaskLoadBalancer(AbstractConfig config){
        this.name = config.getString(RESTSyncConnector.CONNECTION_ID);
        this.tasks = new HashMap<>();
        log.info("Load Balancer for {} is activated",this.name);
        RESTContextRegistry.getInstance().registerLB(this.name, this);
    }

    @Override
    public void register(RESTInputSourceTask object) {
        if (tasks.keySet().contains(object)){
            log.warn("Task {} is already registered in loadbalancer. Just update loadbalancer", object.hashCode());
        }
        this.tasks.put(object, object.loadScore());
        if ((this.appropriateItem == null) || (this.tasks.get(this.appropriateItem) > object.loadScore())) this.appropriateItem = object;
    }

    @Override
    public void deregister(RESTInputSourceTask member) {
        if (!this.tasks.keySet().contains(member))
            log.warn("Loadbalancer requested unregister of unregistered item {}", member.hashCode());
        this.tasks.remove(member);
        log.trace("Task {} is unloaded from loadbalancer", member.hashCode());
        //readdress appropriate member
        if (!this.appropriateItem.equals(member)){
            this.readdress();
        }
    }

    private void readdress(){
        int lowest = Integer.MAX_VALUE;
        for (RESTInputSourceTask task : new ArrayList<RESTInputSourceTask>(tasks.keySet())){
            if (this.tasks.get(task) <= lowest){
                lowest = this.tasks.get(task);
                this.appropriateItem = task;
            }
        }
    }

    @Override
    public RESTInputSourceTask getAppropriate() {
        if (this.appropriateItem == null){
            throw new NoTaskException("no task is registered in loadbalancer. Try wait tasks to ready");
        }
        return this.appropriateItem;
    }

    @Override
    public void updatescore(RESTInputSourceTask member, int newscore) {
        if (!this.tasks.keySet().contains(member)){
            log.error("loadbalancer does not have member task {} but member update is requested, which has no effect.", member.hashCode());
            return;
        }
        this.tasks.put(member, newscore);
        if (this.tasks.get(this.appropriateItem) > newscore) this.appropriateItem = member;
    }    
}
