package com.localxdata.storage;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.localxdata.struct.DataCell;
import com.localxdata.util.LogUtil;


/**
 * If we use a loop to chekc all the member of the list,
 * we cannot user remove function.so we need a lock to 
 * control the concurrency.
 *     1.if we user synchronized(XXX),only a thread can 
 *       query the list,the efficiency is too low~~.
 *       
 *     2.we use 2 flag to make query confilict with remove.
 *       when remove function is called,the query function can only wait....
 *       also ,query function must wait until the remove function finish....
 *       But the same function can do at the same time.
 *       
 *     3.The delete operation priority is very low.Both the add and query opertation
 *       can break remove function (TODO)
 * */

public class DataCellList {
    
	public static final String TAG = "DataCellList";

    private List<DataCell> list;
    private ReentrantReadWriteLock rwl;
    private ReadLock mReadLock;
    private WriteLock mWriteLock;
    
    public DataCellList() {
    	 list = Collections.synchronizedList(new ArrayList<DataCell>());
         rwl = new ReentrantReadWriteLock();
         mReadLock = rwl.readLock();
         mWriteLock = rwl.writeLock();
    }
    
    
    public boolean add(DataCell data) {
        //setRemoveAddFlag();
    	mWriteLock.lock();
        boolean result = list.add(data);
        mWriteLock.unlock();
        
        return result;
    }
    
    public void addAll(DataCellList list) {
    	LogUtil.e(TAG,"addAll function not support");
    	return;
    }
    
    //we use exception for this function...
    public void addAll(int index,DataCellList cell) {
    	LogUtil.e(TAG, "addAll function not support");
    }
    
    public void add(int index,DataCell cell) {
    	LogUtil.e(TAG, "add function not support");
    }
    
    public DataCell remove(int index) {
    	LogUtil.e(TAG, "remove function not support");
    	return null;
    }
    
    public boolean remove(DataCell cell) {
    	mWriteLock.lock();
    	boolean result = list.remove(cell);
    	mWriteLock.unlock();
    	return result;
    }
    
    public void removeRange(int fromIndex,int toIndex) {
    	LogUtil.e(TAG, "removeRange function not support");
    	return;
    }
    
    public void removeAll(DataCellList list) {
    	mWriteLock.lock();
    	this.list.removeAll(list.list);
    	mWriteLock.unlock();
    }
    
    public boolean removeAll(Collection<?> c) {
    	mWriteLock.lock();
    	boolean result = list.removeAll(c);
    	mWriteLock.unlock();
    	
    	return result;
    }
    
    //we use this method to do.......
    public DataCell insertDataCell(Object obj) {
    	mWriteLock.lock();
        DataCell d = new DataCell(obj);
        
        int maxid = 0;
        int size = list.size();
        if(size != 0) {
        	maxid = list.get(size - 1).getId() + 1;
        }else {
        	maxid = 0;
        }
        
        d.setId(maxid);
        list.add(d);
    	mWriteLock.unlock();
    	return d;
    }
       
    public DataCell get(int index) {
    	return list.get(index);
    }
    
    public int size() {
        return list.size();
    }
    
    public void startLoopRead() {
    	this.mReadLock.lock();
    }
    
    public void finishLoopRead() {
    	this.mReadLock.unlock();
    }
    
    public Iterator<DataCell> getIterator() {
    	return this.list.iterator();
    }
    
    public int indexOf(DataCell cell) {
    	startLoopRead();
    	int length = list.size();
    	for(int i = 0;i < length;i++) {
    		if(list.get(i) == cell) {
    			finishLoopRead();
    			return i;
    		}
    	}
    	
    	finishLoopRead();
    	return -1;
    }
}
