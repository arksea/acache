package net.arksea.base;

import akka.dispatch.Futures;
import akka.dispatch.Mapper;
import akka.dispatch.OnComplete;
import net.arksea.base.functional.Consumer2;
import scala.concurrent.ExecutionContext;
import scala.concurrent.Future;

import java.util.List;
import java.util.function.Function;

import static akka.japi.Util.classTag;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public final class FutureUtils {
    private FutureUtils() {}
    public static <T> void completeFutures(ExecutionContext dispatcher, List<Future<T>> futures, Consumer2<Throwable,Iterable<T>> func) {
        Futures.sequence(futures,dispatcher).map(
            new Mapper<Iterable<T>, Iterable<T>>() {
                public Iterable<T> apply(Iterable<T> it) {
                    return it;
                }
            },dispatcher).onComplete(new OnComplete<Iterable<T>>() {
            @Override
            public void onComplete(Throwable ex, Iterable<T> values) throws Throwable {
                func.accept(ex, values);
            }
        },dispatcher);
    }

    public static <T,R> Mapper<T,R> mapper(Function<T,R> func) {
        return new Mapper<T, R>() {
            @Override
            public R apply(T t) {
                return func.apply(t);
            }
        };
    }

    public static <T> Future<List<T>> mapFutures(List<Future<T>> futures, ExecutionContext dispatcher) {
        return Futures.sequence(futures,dispatcher).map(
            mapper((Iterable<T> a) -> a),dispatcher
        ).mapTo(classTag((Class<List<T>>)(Class<?>)List.class));
    }

    public static <T> OnComplete<T> completer(Consumer2<Throwable,T> func) {
        return new OnComplete<T>() {
            @Override
            public void onComplete(Throwable throwable, T t) throws Throwable {
                func.accept(throwable, t);
            }
        };
    }
}
