package com.localxdata.index;

import java.util.*;

import com.localxdata.util.PraseSqlUtil.Action;

public class IndexTree<T extends Comparable> {
    // 定义红黑树的颜色
    private static final boolean RED = Node.RED;
    private static final boolean BLACK = Node.BLACK;

    private Node root;
    private String mDataType;
    private String mFieldName;

    // 两个构造器用于创建排序二叉树
    public IndexTree(String fieldName) {
        root = null;
        mFieldName = fieldName;
    }
    
    public String getDataType() {
    	return mDataType;
    }

    // 添加节点
    public void add(Object obj, T ele) {
        // 如果根节点为null
        if (root == null) {
            root = new Node(obj, ele, null, null, null);
            try {
				mDataType = obj.getClass().getField(mFieldName).getType().getName();
			} catch (SecurityException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (NoSuchFieldException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        } else {
            Node current = root;
            Node parent = null;
            int cmp = 0;
            // 搜索合适的叶子节点，以该叶子节点为父节点添加新节点
            do {
                parent = current;
                cmp = ele.compareTo(current.data);
                // 如果新节点的值大于当前节点的值
                if (cmp > 0) {
                    // 以右子节点作为当前节点
                    current = current.right;
                }
                // 如果新节点的值小于当前节点的值
                else {
                    // 以左子节点作为当前节点
                    current = current.left;
                }
            } while (current != null);
            // 创建新节点
            Node newNode = new Node(obj, ele, parent, null, null);
            // 如果新节点的值大于父节点的值
            if (cmp > 0) {
                // 新节点作为父节点的右子节点
                parent.right = newNode;
            }
            // 如果新节点的值小于父节点的值
            else {
                // 新节点作为父节点的左子节点
                parent.left = newNode;
            }
            // 维护红黑树
            fixAfterInsertion(newNode);
        }
    }

    // 删除节点
    public void remove(T ele) {
        // 获取要删除的节点
        Node target = getNode(ele);
        // 如果被删除节点的左子树、右子树都不为空
        if (target.left != null && target.right != null) {
            // 找到target节点中序遍历的前一个节点
            // s用于保存target节点的左子树中值最大的节点
            Node s = target.left;
            // 搜索target节点的左子树中值最大的节点
            while (s.right != null) {
                s = s.right;
            }
            // 用s节点来代替p节点
            target.data = s.data;
            target = s;
        }
        // 开始修复它的替换节点，如果该替换节点不为null
        Node replacement = (target.left != null ? target.left : target.right);
        if (replacement != null) {
            // 让replacement的parent指向target的parent
            replacement.parent = target.parent;
            // 如果target的parent为null，表明target本身是根节点
            if (target.parent == null) {
                root = replacement;
            }
            // 如果target是其父节点的左子节点
            else if (target == target.parent.left) {
                // 让target的父节点left指向replacement
                target.parent.left = replacement;
            }
            // 如果target是其父节点的右子节点
            else {
                // 让target的父节点right指向replacement
                target.parent.right = replacement;
            }
            // 彻底删除target节点
            target.left = target.right = target.parent = null;

            // 修复红黑树
            if (target.color == BLACK) {
                fixAfterDeletion(replacement);
            }
        }
        // target本身是根节点
        else if (target.parent == null) {
            root = null;
        } else {
            // target没有子节点，把它当成虚的替换节点。
            // 修复红黑树
            if (target.color == BLACK) {
                fixAfterDeletion(target);
            }
            if (target.parent != null) {
                // 如果target是其父节点的左子节点
                if (target == target.parent.left) {
                    // 将target的父节点left设为null
                    target.parent.left = null;
                }
                // 如果target是其父节点的右子节点
                else if (target == target.parent.right) {
                    // 将target的父节点right设为null
                    target.parent.right = null;
                }
                // 将target的parent设置null
                target.parent = null;
            }
        }
    }

    // 根据给定的值搜索节点
    public Node getNode(T ele) {
        // 从根节点开始搜索
        Node p = root;
        while (p != null) {
            int cmp = ele.compareTo(p.data);
            // 如果搜索的值小于当前p节点的值
            if (cmp < 0) {
                // 向左子树搜索
                p = p.left;
            }
            // 如果搜索的值大于当前p节点的值
            else if (cmp > 0) {
                // 向右子树搜索
                p = p.right;
            } else {
                return p;
            }
        }
        return null;
    }

    public Node getNode(Object obj, T ele) {
        Node p = this.root;
        while (p != null) {
            int cmp = ele.compareTo(p.data);

            if (cmp < 0) {
                p = p.left;
            } else if (cmp > 0) {
                p = p.right;
            } else {
                while (p != null) {
                    if (p.obj == obj) {
                        return p;
                    }
                    p = p.equal;
                }
            }
        }

        return null;
    }

    public HashSet<Object>searchNode(int action,T ele) {
    	
    	Node resultNode = getNode(action,ele);
    	
    	HashSet<Object> result = new HashSet<Object>();
    	
    	adjustAfterGetNode(resultNode,result,action,ele);
    	
    	return result;
    	
    }
    
    public Node getNode(int action, T ele) {
        Node p = this.root;

        while (p != null) {
            int cmp = ele.compareTo(p.data);
            switch (action) {
            case Action.SQL_ACTION_EQUAL:
                if (cmp == 0) {
                    return p;
                }

                break;
            case Action.SQL_ACTION_LESS_THAN:
                if (cmp > 0 || cmp == 0) {
                    return p;
                }

                if (cmp < 0) {
                    p = p.left;
                }

                break;
            case Action.SQL_ACTION_MORE_THAN:
                if (cmp < 0 || cmp == 0) {
                    return p;
                }

                if (cmp > 0) {
                    p = p.right;
                }
                break;

            }            
        }

        
        return null;
    }

    // 广度优先遍历
    public List<Node> breadthFirst() {
        Queue<Node> queue = new ArrayDeque<Node>();
        List<Node> list = new ArrayList<Node>();
        if (root != null) {
            // 将根元素入“队列”
            queue.offer(root);
        }
        while (!queue.isEmpty()) {
            // 将该队列的“队尾”的元素添加到List中
            list.add(queue.peek());
            Node p = queue.poll();
            // 如果左子节点不为null，将它入“队列”
            if (p.left != null) {
                queue.offer(p.left);
            }
            // 如果右子节点不为null，将它入“队列”
            if (p.right != null) {
                queue.offer(p.right);
            }
        }
        return list;
    }
    
    //wangsl
    //for example:parent data is 108,and the left child is 96,so if 
    //i want to get all the data less than 100,the proper node is 96.
    //so the nod(96)'s right child (103) may also be selected,
    private void adjustAfterGetNode(Node node,HashSet<Object> result,int action, T ele) {
    	Queue<Node> queue = new ArrayDeque<Node>(); 
        queue.offer(node);
        
        Queue<Node> suspectQueue = new ArrayDeque<Node>();
        
        int count = 0;
        while (!queue.isEmpty() || !suspectQueue.isEmpty()) {
        	//we check suspectQueue first
        	Node p = null;
        	
        	if(!suspectQueue.isEmpty()) {
        		System.out.println("wangsl,emptffffffffff");
        		Node node1 = suspectQueue.peek();
        		int cmp = ele.compareTo(node1.data);
        		System.out.println("suscpect data is " + node1.data);
        		switch(action) {
        		    case Action.SQL_ACTION_LESS_THAN:
        		        if(cmp <= 0) {
        		        	result.add(node1.obj);
        		        }
        		    	break;
        		    	
        		    case Action.SQL_ACTION_MORE_THAN:
        		    	if(cmp >= 0) {
        		    		result.add(node1.obj);
        		    	}
        		    	break;
        		}
        		
        		p = suspectQueue.poll();
        		
        	} else if(!queue.isEmpty()) {
        		Node node1 = queue.peek();
            	//result.add(queue.peek().obj);
            	result.add(node1.obj);
            	System.out.println("data is " + node1.data);
                p = queue.poll();
        	}
        	
            // 如果左子节点不为null，将它入“队列”
            if (p.left != null) {
            	int cmp = ele.compareTo(p.left.data);
            	
            	//TODO
            	switch(action) {
                    case Action.SQL_ACTION_LESS_THAN:
                    	if(cmp > 0) {
                    		queue.offer(p.left);
                    	} else {
                    		if(p.left.right != null) {
                    		    suspectQueue.offer(p.left.right);
                    		}
                    	}
                        break;
                    case Action.SQL_ACTION_MORE_THAN:
                        if(cmp < 0) {
                        	queue.offer(p.left);
                        } else {
                        	if(p.left.left != null) {
                        		suspectQueue.offer(p.left.left);
                        	}
                        }
                        break;
            	}
                //queue.offer(p.left);
            }
            // 如果右子节点不为null，将它入“队列”
            if (p.right != null) {
            	int cmp = ele.compareTo(p.right.data);
            	switch(action) {
                    case Action.SQL_ACTION_LESS_THAN:
                	    if(cmp > 0) {
                		    queue.offer(p.right);
                	    }else {
                    		if(p.right.right != null) {
                    		    suspectQueue.offer(p.right.right);
                    		}
                    	}
                        break;
                    case Action.SQL_ACTION_MORE_THAN:
                        if(cmp < 0) {
                    	    queue.offer(p.right);
                        } else {
                        	if(p.right.left != null) {
                        		suspectQueue.offer(p.right.left);
                        	}
                        }
                        break;
        	    }
                //queue.offer(p.right);
            }
        }
    }
    //wangsl

    // 插入节点后修复红黑树
    private void fixAfterInsertion(Node x) {
        x.color = RED;
        // 直到x节点的父节点不是根，且x的父节点不是红色
        while (x != null && x != root && x.parent.color == RED) {
            // 如果x的父节点是其父节点的左子节点
            if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {
                // 获取x的父节点的兄弟节点
                Node y = rightOf(parentOf(parentOf(x)));
                // 如果x的父节点的兄弟节点是红色
                if (colorOf(y) == RED) {
                    // 将x的父节点设为黑色
                    setColor(parentOf(x), BLACK);
                    // 将x的父节点的兄弟节点设为黑色
                    setColor(y, BLACK);
                    // 将x的父节点的父节点设为红色
                    setColor(parentOf(parentOf(x)), RED);
                    x = parentOf(parentOf(x));
                }
                // 如果x的父节点的兄弟节点是黑色
                else {
                    // 如果x是其父节点的右子节点
                    if (x == rightOf(parentOf(x))) {
                        // 将x的父节点设为x
                        x = parentOf(x);
                        rotateLeft(x);
                    }
                    // 把x的父节点设为黑色
                    setColor(parentOf(x), BLACK);
                    // 把x的父节点的父节点设为红色
                    setColor(parentOf(parentOf(x)), RED);
                    rotateRight(parentOf(parentOf(x)));
                }
            }
            // 如果x的父节点是其父节点的右子节点
            else {
                // 获取x的父节点的兄弟节点
                Node y = leftOf(parentOf(parentOf(x)));
                // 如果x的父节点的兄弟节点是红色
                if (colorOf(y) == RED) {
                    // 将x的父节点设为黑色。
                    setColor(parentOf(x), BLACK);
                    // 将x的父节点的兄弟节点设为黑色
                    setColor(y, BLACK);
                    // 将x的父节点的父节点设为红色
                    setColor(parentOf(parentOf(x)), RED);
                    // 将x设为x的父节点的节点
                    x = parentOf(parentOf(x));
                }
                // 如果x的父节点的兄弟节点是黑色
                else {
                    // 如果x是其父节点的左子节点
                    if (x == leftOf(parentOf(x))) {
                        // 将x的父节点设为x
                        x = parentOf(x);
                        rotateRight(x);
                    }
                    // 把x的父节点设为黑色
                    setColor(parentOf(x), BLACK);
                    // 把x的父节点的父节点设为红色
                    setColor(parentOf(parentOf(x)), RED);
                    rotateLeft(parentOf(parentOf(x)));
                }
            }
        }
        // 将根节点设为黑色
        root.color = BLACK;
    }

    // 删除节点后修复红黑树
    private void fixAfterDeletion(Node x) {
        // 直到x不是根节点，且x的颜色是黑色
        while (x != root && colorOf(x) == BLACK) {
            // 如果x是其父节点的左子节点
            if (x == leftOf(parentOf(x))) {
                // 获取x节点的兄弟节点
                Node sib = rightOf(parentOf(x));
                // 如果sib节点是红色
                if (colorOf(sib) == RED) {
                    // 将sib节点设为黑色
                    setColor(sib, BLACK);
                    // 将x的父节点设为红色
                    setColor(parentOf(x), RED);
                    rotateLeft(parentOf(x));
                    // 再次将sib设为x的父节点的右子节点
                    sib = rightOf(parentOf(x));
                }
                // 如果sib的两个子节点都是黑色
                if (colorOf(leftOf(sib)) == BLACK
                        && colorOf(rightOf(sib)) == BLACK) {
                    // 将sib设为红色
                    setColor(sib, RED);
                    // 让x等于x的父节点
                    x = parentOf(x);
                } else {
                    // 如果sib的只有右子节点是黑色
                    if (colorOf(rightOf(sib)) == BLACK) {
                        // 将sib的左子节点也设为黑色
                        setColor(leftOf(sib), BLACK);
                        // 将sib设为红色
                        setColor(sib, RED);
                        rotateRight(sib);
                        sib = rightOf(parentOf(x));
                    }
                    // 设置sib的颜色与x的父节点的颜色相同
                    setColor(sib, colorOf(parentOf(x)));
                    // 将x的父节点设为黑色
                    setColor(parentOf(x), BLACK);
                    // 将sib的右子节点设为黑色
                    setColor(rightOf(sib), BLACK);
                    rotateLeft(parentOf(x));
                    x = root;
                }
            }
            // 如果x是其父节点的右子节点
            else {
                // 获取x节点的兄弟节点
                Node sib = leftOf(parentOf(x));
                // 如果sib的颜色是红色
                if (colorOf(sib) == RED) {
                    // 将sib的颜色设为黑色
                    setColor(sib, BLACK);
                    // 将sib的父节点设为红色
                    setColor(parentOf(x), RED);
                    rotateRight(parentOf(x));
                    sib = leftOf(parentOf(x));
                }
                // 如果sib的两个子节点都是黑色
                if (colorOf(rightOf(sib)) == BLACK
                        && colorOf(leftOf(sib)) == BLACK) {
                    // 将sib设为红色
                    setColor(sib, RED);
                    // 让x等于x的父节点
                    x = parentOf(x);
                } else {
                    // 如果sib只有左子节点是黑色
                    if (colorOf(leftOf(sib)) == BLACK) {
                        // 将sib的右子节点也设为黑色
                        setColor(rightOf(sib), BLACK);
                        // 将sib设为红色
                        setColor(sib, RED);
                        rotateLeft(sib);
                        sib = leftOf(parentOf(x));
                    }
                    // 将sib的颜色设为与x的父节点颜色相同
                    setColor(sib, colorOf(parentOf(x)));
                    // 将x的父节点设为黑色
                    setColor(parentOf(x), BLACK);
                    // 将sib的左子节点设为黑色
                    setColor(leftOf(sib), BLACK);
                    rotateRight(parentOf(x));
                    x = root;
                }
            }
        }
        setColor(x, BLACK);
    }

    // 获取指定节点的颜色
    private boolean colorOf(Node p) {
        return (p == null ? BLACK : p.color);
    }

    // 获取指定节点的父节点
    private Node parentOf(Node p) {
        return (p == null ? null : p.parent);
    }

    // 为指定节点设置颜色
    private void setColor(Node p, boolean c) {
        if (p != null) {
            p.color = c;
        }
    }

    // 获取指定节点的左子节点
    private Node leftOf(Node p) {
        return (p == null) ? null : p.left;
    }

    // 获取指定节点的右子节点
    private Node rightOf(Node p) {
        return (p == null) ? null : p.right;
    }

    /**
     * 执行如下转换 p r r p q q
     */
    private void rotateLeft(Node p) {
        if (p != null) {
            // 取得p的右子节点
            Node r = p.right;
            Node q = r.left;
            // 将r的左子节点链到p的右节点链上
            p.right = q;
            // 让r的左子节点的parent指向p节点
            if (q != null) {
                q.parent = p;
            }
            r.parent = p.parent;
            // 如果p已经是根节点
            if (p.parent == null) {
                root = r;
            }
            // 如果p是其父节点的左子节点
            else if (p.parent.left == p) {
                // 将r设为p的父节点的左子节点
                p.parent.left = r;
            } else {
                // 将r设为p的父节点的右子节点
                p.parent.right = r;
            }
            r.left = p;
            p.parent = r;
        }
    }

    /**
     * 执行如下转换 p l l p q q
     */
    private void rotateRight(Node p) {
        if (p != null) {
            // 取得p的左子节点
            Node l = p.left;
            Node q = l.right;
            // 将l的右子节点链到p的左节点链上
            p.left = q;
            // 让l的右子节点的parent指向p节点
            if (q != null) {
                q.parent = p;
            }
            l.parent = p.parent;
            // 如果p已经是根节点
            if (p.parent == null) {
                root = l;
            }
            // 如果p是其父节点的右子节点
            else if (p.parent.right == p) {
                // 将l设为p的父节点的右子节点
                p.parent.right = l;
            } else {
                // 将l设为p的父节点的左子节点
                p.parent.left = l;
            }
            l.right = p;
            p.parent = l;
        }
    }

    // 实现中序遍历
    public List<Node> inIterator() {
        return inIterator(root);
    }

    private List<Node> inIterator(Node node) {
        List<Node> list = new ArrayList<Node>();
        // 递归处理左子树
        if (node.left != null) {
            list.addAll(inIterator(node.left));
        }
        // 处理根节点
        list.add(node);
        // 递归处理右子树
        if (node.right != null) {
            list.addAll(inIterator(node.right));
        }
        return list;
    }

}
