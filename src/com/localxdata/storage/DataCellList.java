package com.localxdata.storage;

import java.util.ArrayList;

import com.localxdata.exception.MethodException;
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
    	LogUtil.d("DataCellList", "size is " + this.size());
    	int size = this.size();
    	
    	int maxid = 0;
    	
    	if(size != 0) {
    	    maxid = this.get(size - 1).getId() + 1;
    	}else {
    		maxid = 0;
    	}
    	
        data.setId(maxid);
        setRemoveAddFlag();
        boolean result = super.add(data);
        clearRemoveAddFlag();
        
        return result;
    }
    
    public void addAll(DataCellList list) {
    	int maxid =  this.get(this.size() - 1).getId();
    	
    	setRemoveAddFlag();
        super.addAll(list);
        clearRemoveAddFlag();
        
        //we should change id
        int startIndex = super.indexOf(list.get(0));
        int length = list.size();
        
        for(int i = 0;i < length;i++) {
        	int size = this.size();
        	int setId = 0;
        	if(size != 0) {
        		setId = maxid + i + 1;
        	}else {
        		setId = 0;
        	}
        	
            DataCell t = (DataCell)list.get(i);
            t.setId(setId);
        }
    }
    
    //we use exception for this function...
    public void addAll(int index,DataCellList cell) throws MethodException {
    	LogUtil.e(TAG, "addAll function not support");
    	throw new MethodException();
    }
    
    public void add(int index,DataCell cell) {
    	LogUtil.e(TAG, "add function not support");
    }
    
    public DataCell remove(int index) {
    	//this method is fobbiden
    	setRemoveAddFlag();
    	DataCell result = super.remove(index);
    	clearRemoveAddFlag();
    	return result;
    }
    
    public boolean remove(DataCell cell) {
    	setRemoveAddFlag();
    	boolean result = super.remove(cell);
    	clearRemoveAddFlag();
    	return result;
    }
    
    public void removeRange(int fromIndex,int toIndex) {
    	setRemoveAddFlag();
    	super.removeRange(fromIndex,toIndex);
    	clearRemoveAddFlag();
    }
    
    public void removeAll(DataCellList list) {
    	setRemoveAddFlag();
    	super.removeAll(list);
    	clearRemoveAddFlag();
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
