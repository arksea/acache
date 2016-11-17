package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface ICacheConfig {
    long getIdleTimeout();
    long getLiveTimeout();
    long getMaxBackoff();
}
