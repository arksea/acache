package net.arksea.acache;

import java.util.List;

/**
 *
 * Created by xiaohaixing on 2017/8/13.
 */
public class GetSize<TKey> extends CacheRequest<TKey,List> {

    public GetSize(TKey key) {
        super(key);
    }
}
