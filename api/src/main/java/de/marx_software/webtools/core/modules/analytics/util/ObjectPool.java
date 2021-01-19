package de.marx_software.webtools.core.modules.analytics.util;

/*-
 * #%L
 * webtools-base
 * %%
 * Copyright (C) 2016 - 2019 Thorsten Marx
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */

import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author marx
 */
public abstract class ObjectPool<T> {

    private final Queue<T> objects;

    public ObjectPool() {
        this.objects = new ConcurrentLinkedQueue<>();
    }

    public ObjectPool(Collection<? extends T> objects) {
        this.objects = new ConcurrentLinkedQueue<>(objects);
    }

    public abstract T createExpensiveObject();

    public T borrow() {
        T t;
        if ((t = objects.poll()) == null) {
            t = createExpensiveObject();
        }
        return t;
    }

    public void giveBack(T object) {
        this.objects.offer(object);   // no point to wait for free space, just return
    }
}
