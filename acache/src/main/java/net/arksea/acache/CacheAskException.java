package net.arksea.acache;

/**
 *
 * Created by xiaohaixing on 2017/7/21.
 */
public class CacheAskException extends RuntimeException {
    public CacheAskException() {
        super();
    }
    public CacheAskException(String message) {
        super(message);
    }
    public CacheAskException(String message, Throwable cause) {
        super(message, cause);
    }
    public CacheAskException(Throwable cause) {
        super(cause);
    }
    protected CacheAskException(String message, Throwable cause,
                               boolean enableSuppression,
                               boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
