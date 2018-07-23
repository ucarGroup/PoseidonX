//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.ucar.streamsuite.common.util;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class MapBackedSet<E> extends AbstractSet<E> implements Serializable {
    private static final long serialVersionUID = -8347878570391674042L;
    protected final Map<E, Boolean> map;

    public MapBackedSet(Map<E, Boolean> map) {
        this.map = map;
    }

    public MapBackedSet(Map<E, Boolean> map, Collection<E> c) {
        this.map = map;
        this.addAll(c);
    }

    public int size() {
        return this.map.size();
    }

    public boolean contains(Object o) {
        return this.map.containsKey(o);
    }

    public Iterator<E> iterator() {
        return this.map.keySet().iterator();
    }

    public boolean add(E o) {
        return this.map.put(o, Boolean.TRUE) == null;
    }

    public boolean remove(Object o) {
        return this.map.remove(o) != null;
    }

    public void clear() {
        this.map.clear();
    }
}
