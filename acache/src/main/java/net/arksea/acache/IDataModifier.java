package net.arksea.acache;

/**
 *
 * Created by arksea on 2016/11/17.
 */
public interface IDataModifier<TData> {
    TData apply(TData old);
}