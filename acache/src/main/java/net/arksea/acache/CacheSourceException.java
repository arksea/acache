package net.arksea.acache;

/**
 *
 * Created by xiaohaixing on 2017/7/21.
 */
public class CacheSourceException extends RuntimeException {
    public CacheSourceException() {
        super();
    }
    public CacheSourceException(String message) {
        super(message);
    }
    public CacheSourceException(String message, Throwable cause) {
        super(message, cause);
    }
    public CacheSourceException(Throwable cause) {
        super(cause);
    }
    protected CacheSourceException(String message, Throwable cause,
                                   boolean enableSuppression,
                                   boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
