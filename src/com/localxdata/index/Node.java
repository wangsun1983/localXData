package com.localxdata.index;

import java.util.ArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.localxdata.struct.DataCell;

public class Node<T extends Comparable> {
    public static final boolean RED = false;
    public static final boolean BLACK = true;
    DataCell dataCell;
    T data;
    Node<?> parent;
    Node left;
    Node right;
    Node equalParent;
    ArrayList<Node> equalList;
    boolean color = true;
    private boolean isDelete = false;
    
    //if the node was marked as deleted,
    //mVisitRef should be used to record the times
    //that the noded was visited.
    //if mVisitRef is over a particular value(MAX_VISIT_REF)
    //the node will be delete really.
    private static final int MAX_VISIT_REF = 200;
    
    int mVisitRef = 0; 

    public Node(DataCell dataCell, T data, Node parent, Node left, Node right,ArrayList<Node>equalList,Node equalparent) {
        this.dataCell = dataCell;
        this.data = data;
        this.parent = parent;
        this.left = left;
        this.right = right;
        this.equalList = equalList;
        this.equalParent = equalparent;
    }

    public Node(Node d) {
        this.dataCell = d.dataCell;
        this.data = (T) d.data;
        this.parent = d.parent;
        this.left = d.left;
        this.right = d.right;
        this.equalList = d.equalList;
        this.equalParent = d.equalParent;
    }
    
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj.getClass() == Node.class) {
            Node target = (Node) obj;

            return (this.data.equals(target.data))
                    && (this.color == target.color)
                    && (this.left == target.left)
                    && (this.right == target.right)
                    && (this.parent == target.parent);
        }
        return false;
    }
    
    public void markDelete() {
    	isDelete = true;
    }
    
    public boolean isDelete() {
    	return isDelete;
    }
    
    public void reUse() {
    	isDelete = true;
    }
    
    public void addVisitRef() {
    	mVisitRef++;
    }
    
    public boolean isNeedRealDelete() {
    	if(mVisitRef == MAX_VISIT_REF) {
    		return true; 
    	}
    	
    	return false;
    }
}