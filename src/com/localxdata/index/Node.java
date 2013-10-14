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
    ArrayList<Node> equalList;
    boolean color = true;
    boolean isDelete = false;

    public Node(DataCell dataCell, T data, Node parent, Node left, Node right,ArrayList<Node>equalList) {
        this.dataCell = dataCell;
        this.data = data;
        this.parent = parent;
        this.left = left;
        this.right = right;
        this.equalList = equalList;
    }

    public Node(Node d) {
        this.dataCell = d.dataCell;
        this.data = (T) d.data;
        this.parent = d.parent;
        this.left = d.left;
        this.right = d.right;
        this.equalList = d.equalList;
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
    
    public void setDelete() {
    	isDelete = true;
    }
    
    public boolean isDelete() {
    	return isDelete;
    }
}