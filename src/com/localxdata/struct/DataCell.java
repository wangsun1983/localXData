package com.localxdata.struct;

import java.util.ArrayList;

import com.localxdata.index.Node;

public class DataCell {
    public static final int DATA_IDLE = 0;
    public static final int DATA_UPDATE = 1;
    public static final int DATA_DELETE = 2;
    public static final int DATA_INSERT = 3;
    
    private int dataState = DATA_INSERT;
    private int id = 0;
    
    public Object obj;
    
    public ArrayList<Node> nodeList; //we can use this node to remove index
    
    public DataCell(Object obj) {
        this.obj = obj;
        
        nodeList = new ArrayList<Node>();
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
    
    public void addNode(Node node) {
    	//this.node = node;
    	nodeList.add(node);
    }
    
    public ArrayList<Node> getNodeList() {
    	return nodeList;
    }
}
