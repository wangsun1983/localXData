package com.localxdata.storage;

import java.util.ArrayList;

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

public class DataCellList extends ArrayList<DataCell>{
    
	public static final String TAG = "DataCellList";
	
    private static final long serialVersionUID = 1L;
    
    private int currentInLoop = 0;
    private int currentInRemoving = 0;
    
    private Object mWaitForLoopObj = new Object();
    private Object mWaitForRemoveAddObj = new Object();
    
    private int size = 0;
    private DataCell mLastCell;
    
    public void enterLooper() {
    	
    	//DebugThread debugThread = new DebugThread();
    	//debugThread.start();
    	
    	if(currentInRemoving != 0) {
    		try {
    			synchronized(mWaitForRemoveAddObj) {
    			    mWaitForRemoveAddObj.wait();
    			}
			} catch (InterruptedException e) {
				LogUtil.e(TAG, "setRemovingFlag error" + e.toString());
			}
    	}
    	
    	currentInLoop++;
    }
    
    public void leaveLooper() {
    	currentInLoop--;
    	
    	if(currentInLoop == 0) {
    		synchronized(mWaitForLoopObj) {
    	        mWaitForLoopObj.notify();
    		}
	    }
    }
    
    public boolean add(DataCell data) {
        setRemoveAddFlag();
        boolean result = super.add(data);
        clearRemoveAddFlag();
        
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
    	setRemoveAddFlag();
    	boolean result = super.remove(cell);
    	size--;
    	
    	if(mLastCell == cell) {
    		mLastCell = get(size - 1);
    	}
    	clearRemoveAddFlag();
    	return result;
    }
    
    public void removeRange(int fromIndex,int toIndex) {
    	LogUtil.e(TAG, "removeRange function not support");
    	return;
    }
    
    public void removeAll(DataCellList list) {
    	setRemoveAddFlag();
    	super.removeAll(list);
    	
    	size = size - list.size();
    	
    	if(list.indexOf(mLastCell) < 0) {
    		mLastCell = get(size - 1);
    	}
    	
    	clearRemoveAddFlag();
    }
    
    //we use this method to do.......
    public DataCell insertDataCell(Object obj) {
    	setRemoveAddFlag();
        DataCell d = new DataCell(obj);
        super.add(d);
                
        int maxid = 0;
    	if(size != 0) {
    		maxid = mLastCell.getId() + 1;
    	}else {
    		maxid = 0;
    	}
    	
    	d.setId(maxid);
    	mLastCell = d;
    	size++;
    	clearRemoveAddFlag();
    	return d;
    }
       
    public int size() {
        return size;
    }
    
    private void setRemoveAddFlag() {
    	if(currentInLoop != 0) {
    		try {
    			synchronized(mWaitForLoopObj) {
				    mWaitForLoopObj.wait();
    			}
			} catch (InterruptedException e) {
				LogUtil.e(TAG, "setRemovingFlag error" + e.toString());
			}
    	}
    	
    	currentInRemoving++;
    }
    
    private void clearRemoveAddFlag() {
    	currentInRemoving--;
    	if(currentInRemoving == 0) {
    		synchronized(mWaitForRemoveAddObj) {
    		    mWaitForRemoveAddObj.notify();
    		}
    	}
    }
    
    public void dump() {
    	LogUtil.d(TAG, "currentInLoop is " + currentInLoop);
    	LogUtil.d(TAG, "currentInRemoving is " + currentInRemoving);
    }
    
    class DebugThread extends Thread {
    	public void run() {
    		while(true) {
    		    dump();
    		    try {
				    Thread.sleep(1000);
			    } catch (InterruptedException e) {
				    // TODO Auto-generated catch block
				    e.printStackTrace();
			    }
    		}
    	}
    }
}
