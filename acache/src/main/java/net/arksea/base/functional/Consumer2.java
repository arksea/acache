package net.arksea.base.functional;

import java.util.Objects;

/**
 *
 * Created by arksea on 2016/11/17.
 */

@FunctionalInterface
public interface Consumer2<T1,T2> {

    void accept(T1 t1,T2 t2);
    default Consumer2<T1,T2> andThen(Consumer2<? super T1,? super T2> after) {
        Objects.requireNonNull(after);
        return (T1 t1,T2 t2) -> { accept(t1,t2); after.accept(t1,t2); };
    }
}