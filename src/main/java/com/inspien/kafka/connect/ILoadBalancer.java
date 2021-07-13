package com.inspien.kafka.connect;


/**
 * An interface for load balancers. LoadBalancers could distribute loads to {@link ILoadBalancable}s.
 */
public interface ILoadBalancer<T extends ILoadBalancable> {

    /**
     * register object to loadbalancer
     * @param object object pointer which could consume data.
     */
    public void register(T object);

    /**
     * unregister member.
     * @param member
     */
    public void deregister(T member);

    /**
     * get best object, which has lowest load score
     * @return The member object with lowest load score
     */
    public T getAppropriate();

    /**
     * update score of an object to new value
     * @param member member object to update score
     * @param newscore new score
     */
    public void updatescore(T member, int newscore);
}
