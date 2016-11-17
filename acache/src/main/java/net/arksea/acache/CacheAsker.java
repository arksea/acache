package net.arksea.acache;

import akka.actor.ActorRef;
import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import akka.japi.tuple.Tuple3;
import akka.japi.tuple.Tuple4;
import akka.pattern.Patterns;
import net.arksea.base.functional.Consumer2;
import net.arksea.base.functional.Consumer3;
import scala.Tuple2;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static akka.japi.Util.classTag;
import static net.arksea.base.FutureUtils.mapper;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public final class CacheAsker {

    private final long timeout;
    private final ExecutionContext dispatcher;

    public CacheAsker(ExecutionContext dispatcher, long timeout) {
        this.dispatcher = dispatcher;
        this.timeout = timeout;
    }

    public <K,V,R> Future<R> askThenMap(K key,ActorRef cacheActor,Function<V,R> func) {
        Future<DataResult<K, V>> future = Patterns.ask(cacheActor, new GetData<K>(cacheActor, key), timeout)
            .mapTo(classTag((Class<DataResult<K, V>>) (Class<?>) DataResult.class));
        return future.map(mapper(
            it -> {
                if (it.failed == null) {
                    return func.apply(it.data);
                } else {
                    throw new RuntimeException("获取数据失败",it.failed);
                }
            }
        ),dispatcher);
    }

    public <R,T1,T2> Future<R> askThenMap(Function<Tuple2<T1,T2>,R> function,
                                             GetData get1,GetData get2) {
        Future<DataResult> f1 = CacheActor.ask(get1,timeout);
        Future<DataResult> f2 = CacheActor.ask(get2,timeout);
        List<Future<DataResult>> futures = new ArrayList<>(2);
        futures.add(f1);
        futures.add(f2);
        return ask(futures).map(
            mapper((List<DataResult> it) -> {
                for (DataResult d: it) {
                    if (d.failed != null) {
                        throw new RuntimeException("获取数据失败", d.failed);
                    }
                }
                Tuple2 tuple = Tuple2.apply(it.get(0).data,it.get(1).data);
                return function.apply(tuple);
            }),dispatcher);
    }

    public <R,T1,T2,T3> Future<R> askThenMap(Function<Tuple3<T1,T2,T3>,R> function,
                                             GetData get1,GetData get2,GetData get3) {
        Future<DataResult> f1 = CacheActor.ask(get1,timeout);
        Future<DataResult> f2 = CacheActor.ask(get2,timeout);
        Future<DataResult> f3 = CacheActor.ask(get3,timeout);
        List<Future<DataResult>> futures = new ArrayList<>(3);
        futures.add(f1);
        futures.add(f2);
        futures.add(f3);
        return ask(futures).map(
            mapper((List<DataResult> it) -> {
                for (DataResult d: it) {
                    if (d.failed != null) {
                        throw new RuntimeException("获取数据失败", d.failed);
                    }
                }
                Tuple3 tuple = Tuple3.apply(it.get(0).data,it.get(1).data,it.get(2).data);
                return function.apply(tuple);
            }),dispatcher);
    }

    public <R,T1,T2,T3,T4> Future<R> askThenMap(Function<Tuple4<T1,T2,T3,T4>,R> function,
                                             GetData get1,GetData get2,GetData get3,GetData get4) {
        Future<DataResult> f1 = CacheActor.ask(get1,timeout);
        Future<DataResult> f2 = CacheActor.ask(get2,timeout);
        Future<DataResult> f3 = CacheActor.ask(get3,timeout);
        Future<DataResult> f4 = CacheActor.ask(get3,timeout);
        List<Future<DataResult>> futures = new ArrayList<>(4);
        futures.add(f1);
        futures.add(f2);
        futures.add(f3);
        futures.add(f4);
        return ask(futures).map(
            mapper((List<DataResult> it) -> {
                for (DataResult d: it) {
                    if (d.failed != null) {
                        throw new RuntimeException("获取数据失败",d.failed);
                    }
                }
                Tuple4 tuple = Tuple4.apply(it.get(0).data,it.get(1).data,it.get(2).data,it.get(3).data);
                return function.apply(tuple);
            }),dispatcher);
    }

    private Future<List<DataResult>> ask(List<Future<DataResult>> futures) {
        return Futures.sequence(futures, dispatcher).map(
            new Mapper<Iterable<DataResult>, Iterable<DataResult>>() {
                public Iterable<DataResult> apply(Iterable<DataResult> it) {
                    return it;
                }
            }, dispatcher).mapTo(classTag((Class<List<DataResult>>) (Class<?>) List.class));
    }

    public <K,V> void askThenProcess(K key,ActorRef cacheActor,Consumer<V> onSuccess, Consumer<Throwable> onFailed) {
        Future<DataResult<K,V>> future = Patterns.ask(cacheActor, new GetData<K>(cacheActor, key), timeout)
            .mapTo(classTag((Class<DataResult<K,V>>)(Class<?>)DataResult.class));
        future.onComplete(
            new OnComplete<DataResult<K,V>>() {
                @Override
                public void onComplete(Throwable ex, DataResult<K,V> dataResult) throws Throwable {
                    if (ex == null) {
                        if (dataResult.failed == null) {
                            onSuccess.accept(dataResult.data);
                        } else {
                            onFailed.accept(dataResult.failed);
                        }
                    } else {
                        onFailed.accept(ex);
                    }
                }
            }, dispatcher);
    }

    public <K1,V1,K2,V2>
    void askThenProcess2(Consumer2<V1,V2> onSuccess,
                         Consumer<Throwable> onFailed,
                         GetData<K1> get1,GetData<K2> get2) {
        this.askThenProcess(onSuccess,onFailed,get1,get2);
    }

    public <K1,V1,K2,V2,K3,V3>
    void askThenProcess3(Consumer3<V1,V2,V3> onSuccess,
                         Consumer<Throwable> onFailed,
                         GetData<K1> get1,GetData<K2> get2,GetData<K3> get3) {
        this.askThenProcess(onSuccess,onFailed,get1,get2,get3);
    }

    void askThenProcess(Object onSuccess, Consumer<Throwable> onFailed, GetData ...gets) {
        List<Future<DataResult>> futures = Arrays.asList(gets).stream().map(
            it -> Patterns.ask(it.cacheActor, it, timeout).mapTo(classTag((Class<DataResult>)(Class<?>)DataResult.class))
        ).collect(Collectors.toList());

        Future<List<DataResult>> future =
            Futures.sequence(futures, dispatcher).map(
                new Mapper<Iterable<DataResult>, Iterable<DataResult>>() {
                    public Iterable<DataResult> apply(Iterable<DataResult> it) {
                        return it;
                    }
                },dispatcher).mapTo(classTag((Class<List<DataResult>>) (Class<?>) List.class));

        future.onComplete(
            new OnComplete<List<DataResult>>() {
                @Override
                public void onComplete(Throwable ex, List<DataResult> dataList) throws Throwable {
                    if (ex == null) {
                        Throwable failed = null;
                        for (DataResult d: dataList) {
                            if (d.failed != null) {
                                failed = d.failed;
                                break;
                            }
                        }
                        if (failed == null) {
                            if (onSuccess instanceof Consumer2) {
                                handleSuccess(dataList,(Consumer2)onSuccess);
                            } else if (onSuccess instanceof Consumer3) {
                                handleSuccess(dataList, (Consumer3) onSuccess);
                            }
                        } else {
                            onFailed.accept(failed);
                        }
                    } else {
                        onFailed.accept(ex);
                    }
                }
            },dispatcher);
    }

    private void handleSuccess(List<DataResult> dataList,Consumer2 onSuccess) {
        DataResult d1 = dataList.get(0);
        DataResult d2 = dataList.get(1);
        onSuccess.accept(d1.data,d2.data);
    }

    private void handleSuccess(List<DataResult> dataList,Consumer3 onSuccess) {
        DataResult d1 = dataList.get(0);
        DataResult d2 = dataList.get(1);
        DataResult d3 = dataList.get(2);
        onSuccess.accept(d1.data,d2.data,d3.data);
    }
}
