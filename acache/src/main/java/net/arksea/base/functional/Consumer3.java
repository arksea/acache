package net.arksea.base.functional;

import java.util.Objects;

/**
 *
 * Created by arksea on 2016/11/17.
 */

@FunctionalInterface
public interface Consumer3<T1,T2,T3> {
    void accept(T1 t1,T2 t2,T3 t3);
    default Consumer3<T1,T2,T3> andThen(Consumer3<? super T1,? super T2,? super T3> after) {
        Objects.requireNonNull(after);
        return (T1 t1,T2 t2,T3 t3) -> { accept(t1,t2,t3); after.accept(t1,t2,t3); };
    }
}