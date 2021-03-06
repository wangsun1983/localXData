package com.localxdata.struct;

import java.util.ArrayList;
import java.util.HashMap;

import com.localxdata.index.Node;

public class DataCell {
    public static final byte DATA_IDLE = 0;
    public static final byte DATA_UPDATE = 1;
    public static final byte DATA_DELETE = 2;
    public static final byte DATA_INSERT = 3;
    
    private int dataState = DATA_INSERT;
    private int id = 0;
    
    public Object obj;
    
    //because a DataCell may have some indexes,so we should use a list
    //to save all the node.
    public HashMap<String,Node> nodeMap; 
    
    public DataCell(Object obj) {
        this.obj = obj;
    }
    
    public void setId(int id) {
        this.id = id;
    } 
    
    public int getId() {
        return this.id;
    }
    
    public void setState(int state) {
        this.dataState = state;
    }
    
    public int getState() {
        return this.dataState;
    }
    
    public void addNode(String indexName,Node node) {
    	if(nodeMap == null) {
    		nodeMap = new HashMap<String,Node>();
    	}
    	
    	nodeMap.put(indexName, node);
    }
    
    public HashMap<String,Node> getNodeMap() {
    	return nodeMap;
    }
}
