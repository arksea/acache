package net.arksea.acache;

import akka.routing.ConsistentHashingRouter;

import java.io.Serializable;

/**
 * Created by xiaohaixing_dian91 on 2017/3/30.
 */
public interface ICacheRequest<TKey,TData> extends ConsistentHashingRouter.ConsistentHashable,Serializable {
}
