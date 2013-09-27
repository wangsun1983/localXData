package com.localxdata.index;

import com.localxdata.struct.DataCell;

public class Node<T extends Comparable> {
    public static final boolean RED = false;
    public static final boolean BLACK = true;
    DataCell dataCell;
    T data;
    Node parent;
    Node left;
    Node right;
    Node equal;
    boolean color = true;

    public Node(DataCell dataCell, T data, Node parent, Node left, Node right) {
        this.dataCell = dataCell;
        this.data = data;
        this.parent = parent;
        this.left = left;
        this.right = right;
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
}