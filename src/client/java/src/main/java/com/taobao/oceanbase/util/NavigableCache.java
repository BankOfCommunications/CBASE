package com.taobao.oceanbase.util;

import java.io.UnsupportedEncodingException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

import com.taobao.oceanbase.vo.inner.ObRow;

public class NavigableCache<K, V> {

	private int capacity;
	private NavigableMap<K, Value<V>> map = new TreeMap<K, Value<V>>();
	private ReentrantLock lock = new ReentrantLock();
	private int expireTime = Const.INVALID_RETRY_TIME;

	private final int MIN_CAPACITY = 15;
	private static final int MIN_EXPIRE_TIME = 10;

	public NavigableCache(int capacity) {
		if (capacity < MIN_CAPACITY)
			throw new IllegalArgumentException(
					"capacity is less than MIN_CAPACITY");
		this.capacity = capacity;
	}
	
	public NavigableCache(int capacity, int expireTime) {
		if (capacity < MIN_CAPACITY)
			throw new IllegalArgumentException(
					"capacity is less than MIN_CAPACITY");
		this.capacity = capacity;
		this.expireTime = expireTime;
		if (expireTime < MIN_EXPIRE_TIME)
			expireTime = MIN_EXPIRE_TIME;
	}

	public V put(K key, V value) {
		lock.lock();
		try {
			if (capacity <= map.size()) {
				this.remove();
			} 
			map.put(key, new Value<V>(value));
			return value;
		} finally {
			lock.unlock();
		}
	}

	public V get(K key) {
		lock.lock();
		try {
			Map.Entry<K, Value<V>> start = map.lowerEntry(key);
			Map.Entry<K, Value<V>> end = map.ceilingEntry(key);
			if (start == null || end == null) {
				return null;
			}
			return end.getValue().get();
		} finally {
			lock.unlock();
		}
	}
	
	public void removeIfExpired(K key) {
		lock.lock();
		try {
			Map.Entry<K, Value<V>> end = map.ceilingEntry(key);
			if (end != null) {
				Value<V> v = end.getValue();
				if (v != null && v.isExpired(expireTime)) {
					map.put(key, new Value<V>(null));
				}
			}
		} finally {
			lock.unlock();
		}
	}

	// remove the useLessEntrys
	private void remove() {
		Map.Entry<K, Value<V>>[] useLessEntrys = getUseLessEntrys();
		for (Map.Entry<K, Value<V>> entry : useLessEntrys) {
			map.remove(entry.getKey());
			keyRemoved(entry.getKey());
		}
	}

	private Map.Entry<K, Value<V>>[] getUseLessEntrys() {
		Map.Entry<K, Value<V>>[] useLessEntrys = new Map.Entry[MIN_CAPACITY];
		int count = 0;
		for (Map.Entry<K, Value<V>> entry : map.entrySet()) {
			if (count < MIN_CAPACITY) {
				useLessEntrys[count++] = entry;
			} else {
				int max = getMaxEntry(useLessEntrys);
				if (entry.getValue().count < useLessEntrys[max].getValue().count) {
					useLessEntrys[max] = entry;
				}
			}
		}
		return useLessEntrys;
	}

	private int getMaxEntry(Map.Entry<K, Value<V>>[] mins) {
		int max = 0;
		for (int index = 1; index < mins.length; index++) {
			if (mins[max].getValue().count < mins[index].getValue().count) {
				max = index;
			}
		}
		return max;
	}

	private static class Value<V> {
		private int count;
		private final V value;
		private long ts;

		private Value(V value) {
			this.value = value;
			ts = System.currentTimeMillis();
		}

		public V get() {
			count++;
			return value;
		}
		
		public boolean isExpired(int expireTime) {
			long now = System.currentTimeMillis();
			return (ts + expireTime) > now;
		}
	}

	public K getNextKey(K key) {
		lock.lock();
		try {
			return map.higherKey(key);
		} finally {
			lock.unlock();
		}
	}
	
	
	public void keyRemoved(K key) {
		K nextKey = getNextKey(key);
		if (nextKey != null) {
			Value<V> nextValue = map.get(nextKey);
			if (nextValue != null) {
				V v = nextValue.get();
				if (v instanceof ObRow) {
					ObRow tablet = (ObRow)v;
					tablet.setFull(false);
				}
			}
		}
	}
	
	public String toString(){
		String content = "";
		try {
			for (Map.Entry<K, Value<V>> entry : map.entrySet()) {
				String key = null;
				key = Helper.toHex(entry.getKey().toString()
						.getBytes(Const.NO_CHARSET));
				content += "[" + key + "]--[" + entry.getValue().get();
			}
		} catch (UnsupportedEncodingException e) {
		}
		return content;
	}
	
}