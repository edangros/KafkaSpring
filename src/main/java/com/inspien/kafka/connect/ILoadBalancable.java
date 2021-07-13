package com.inspien.kafka.connect;


/**
 * Interface for objects could be controlled by {@link ILoadBalancer}s.
 */
public interface ILoadBalancable {

    /**
     * The load score of this worker. higher value makes lowest priority for future job allocation in {@link ILoadBalancer}
     * @return load score of this worker.
     */
    public int loadScore();

}
