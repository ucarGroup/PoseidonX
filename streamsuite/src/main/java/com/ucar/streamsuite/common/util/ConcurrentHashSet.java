//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package com.ucar.streamsuite.common.util;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class ConcurrentHashSet<E> extends MapBackedSet<E> {
    private static final long serialVersionUID = 8518578988740277828L;

    public ConcurrentHashSet() {
        super(new ConcurrentHashMap());
    }

    public ConcurrentHashSet(Collection<E> c) {
        super(new ConcurrentHashMap(), c);
    }

    public boolean add(E o) {
        Boolean answer = (Boolean)((ConcurrentMap)this.map).putIfAbsent(o, Boolean.TRUE);
        return answer == null;
    }
}
