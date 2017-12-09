package net.arksea.base;

import java.io.Serializable;
import java.util.UUID;

/**
 *
 * Created by xiaohaixing on 2017/11/6.
 */

/**
 *
 * @param <T> 指定请求的返回结果类型
 */
public class ServiceRequest<T> implements Serializable {
    public final String reqid;
    public ServiceRequest() {
        this.reqid = UUID.randomUUID().toString();
    }
    public ServiceRequest(String reqid) {
        if (reqid == null) {
            this.reqid = UUID.randomUUID().toString();
        } else {
            this.reqid = reqid;
        }
    }
}
