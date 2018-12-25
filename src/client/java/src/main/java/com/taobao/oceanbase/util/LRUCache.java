package com.taobao.oceanbase.util;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.SoftReference;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public class LRUCache<K, V> {

	final private int capacity;
	final private Map<K, Reference<V>> map;
	final private ReentrantLock lock = new ReentrantLock();
	final private ReferenceQueue<V> queue = new ReferenceQueue<V>();

	public LRUCache(int capacity) {
		this.capacity = capacity;
		map = new LinkedHashMap<K, Reference<V>>(capacity, 1f, true) {
			@Override
			protected boolean removeEldestEntry(
					Map.Entry<K, Reference<V>> eldest) {
				return this.size() > LRUCache.this.capacity;
			}
		};
	}

	public V put(K key, V value) {
		lock.lock();
		try {
			map.put(key, new SoftReference<V>(value, queue));
			return value;
		} finally {
			lock.unlock();
		}
	}

	public V get(K key) {
		lock.lock();
		try {
			queue.poll();
			return map.get(key).get();
		} finally {
			lock.unlock();
		}
	}

	public void remove(K key) {
		lock.lock();
		try {
			map.remove(key);
		} finally {
			lock.unlock();
		}
	}

}