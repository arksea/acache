package net.arksea.base;

import java.io.Serializable;

/**
 *
 * Created by xiaohaixing on 2017/11/6.
 */
public class ServiceResponse<T> implements Serializable {
    public final int code;
    public final String msg;
    public final T result;
    public final String reqid;
    public ServiceResponse(int code, String msg, String reqid) {
        this.code = code;
        this.reqid = reqid;
        this.msg = msg;
        this.result = null;
    }
    public ServiceResponse(int code, String msg, String reqid, T result) {
        this.code = code;
        this.reqid = reqid;
        this.msg = msg;
        this.result = result;
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("code=").append(code).append(",msg=").append(msg)
          .append(",reqid=").append(reqid).append(",result=").append(result);
        return sb.toString();
    }
}
