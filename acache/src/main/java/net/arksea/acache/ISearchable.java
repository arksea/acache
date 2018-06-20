package net.arksea.acache;

import java.util.List;

/**
 *
 * Created by xiaohaixing on 2017/8/10.
 */
public interface ISearchable<V,C> {
    V findOne(C condition);
    List<V> findAll(C condition);
    List<V> getAll();
}
