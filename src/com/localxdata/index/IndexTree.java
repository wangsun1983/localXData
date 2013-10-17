package com.localxdata.index;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.localxdata.sql.SqlUtil;
import com.localxdata.storage.DataCellList;
import com.localxdata.storage.StorageNozzle;
import com.localxdata.struct.DataCell;
import com.localxdata.util.PraseSqlUtil.Action;

public class IndexTree<T extends Comparable> {
    //Define the color
    private static final boolean RED = Node.RED;
    private static final boolean BLACK = Node.BLACK;
    
    private Node root;
    private IndexTreeDaemon mDaemon;
    
    private String className = null;
    
    private Lock writeOrReadLock = new ReentrantLock();
    private Lock queryLock = new ReentrantLock();
    
    class IndexTreeDaemon extends Thread {
    	
    	private Object waitObj;
    	
    	private ArrayList<Node>removeList = new ArrayList<Node>();
    	
    	public IndexTreeDaemon() {
    		waitObj = new Object();
    	}
    	
    	public void addRemoveNode(Node node) {
    		synchronized(removeList) {
    		    removeList.add(node);
    		}
    		
    	    synchronized(waitObj) {
    	    	waitObj.notify();
    	    }	
    	}
    	
    	public void run() {
            while(true) {
            	if(removeList.size() == 0) {
            		synchronized(waitObj) {
            		    try {
							waitObj.wait();
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
            		}
            	}
            	
            	Node n = null;
            	synchronized(removeList) {
            		n = removeList.remove(0);
            	}
            	
            	if(n != null) {
            	    remove(n);
            	}
            }                 	
    	}
    }

    public IndexTree(String className) {
    	this.className = className;
    	
        root = null;
        mDaemon = new IndexTreeDaemon();
        mDaemon.setDaemon(true);
        mDaemon.start();
    }

    public void add(DataCell dataCell, T ele) {

    	writeOrReadLock.lock();
    	queryLock.lock();
    	
        if (root == null) {
            root = new Node(dataCell, ele, null, null, null,null,null);
            dataCell.addNode(root);
        } else {
            Node current = root;
            Node parent = null;
            int cmp = 0;

            do {
            	//If node can be replace,we should replace the node instead of deleting.....
            	if(current.isDelete) {
            		
            	}

                parent = current;
                cmp = ele.compareTo(current.data);

                if (cmp > 0) {
                    current = current.right;
                } else if(cmp == 0) {
                	break;
                } else if(cmp < 0){

                    current = current.left;
                }
            } while (current != null);

            Node newNode = new Node(dataCell, ele, parent, null, null,null,null);
            dataCell.addNode(newNode);
            
            if (cmp > 0) {
                parent.right = newNode;
                fixAfterInsertion(newNode);
            } else if(cmp == 0) {
            	if(parent.equalList == null) {
            		parent.equalList = new ArrayList<Node>();
            	}
                parent.equalList.add(newNode);
                newNode.equalParent = parent;
                
            } else {
                parent.left = newNode;
                fixAfterInsertion(newNode);
            }
        }
        
        writeOrReadLock.unlock();
    	queryLock.unlock();
    }

    
    public void remove(Node target) {
    	
    	writeOrReadLock.lock();
    	queryLock.lock();
    	
    	if(target.equalParent != null) {
    		target.equalParent.equalList.remove(target);
    		target.parent = null;
    		
    		writeOrReadLock.unlock();
        	queryLock.unlock();
    		return;
    	}
    	
    	//if the equalist has the same value;   	
    	if(target.equalList != null && target.equalList.size() != 0) {
    		Node placement = (Node) target.equalList.remove(0);
    		placement.equalList = target.equalList;
    		
            if (target.parent == null) {
                root = placement;
            } else if (target == target.parent.left) {
                target.parent.left = placement;
                placement.parent = target.parent;
            } else {        
                target.parent.right = placement;
                placement.parent = target.parent;
            }
            
            for(Object obj:placement.equalList) {
            	Node _n = (Node)obj;
            	_n.equalParent = placement;
            }
            
            target.left = target.right = target.parent = null;
            target.equalList = null;

            placement.color = target.color;
            return;
    	}
    	
    	
        if (target.left != null && target.right != null) {
            Node s = target.left;

            while (s.right != null) {
                s = s.right;
            }

            target.data = s.data;
            target = s;
        }
        
        Node replacement = (target.left != null ? target.left : target.right);
        if (replacement != null) {
        
            replacement.parent = target.parent;
        
            if (target.parent == null) {
                root = replacement;
            }
        
            else if (target == target.parent.left) {
                target.parent.left = replacement;
            }
        
            else {        
                target.parent.right = replacement;
            }
                        
            target.left = target.right = target.parent = null;

            if (target.color == BLACK) {
                fixAfterDeletion(replacement);
            }
        }

        else if (target.parent == null) {
            root = null;
        } else {
            if (target.color == BLACK) {
                fixAfterDeletion(target);
            }
            if (target.parent != null) {

                if (target == target.parent.left) {
                    target.parent.left = null;
                }
                else if (target == target.parent.right) {
                    target.parent.right = null;
                }
                target.parent = null;
            }
        }
        
    	writeOrReadLock.unlock();
    	queryLock.unlock();
    }

    //public void removeByIndex(int action, T ele) {
    //	if(queryLock.tryLock()) {
    //		writeOrReadLock.lock();
    //	} else {
    //		queryLock.unlock();
    //	}
    	
    //	HashSet<Object> list = new HashSet<Object>();
    //	nodeToListByCondition(this.root,list,action,ele,DELETE,false);
    //	writeOrReadLock.unlock();
    //}
    
    public HashSet<Object> getNode(int action,T ele,int reason) {
    	if(queryLock.tryLock()) {
    		writeOrReadLock.lock();
    	} else {
    		queryLock.unlock();
    	}
    	
    	HashSet<Object> list = new HashSet<Object>();
    	nodeToListByCondition(this.root,list,action,ele,reason);
    	
    	writeOrReadLock.unlock();
    	return list;
    }
    
    
    private void nodeToList(Node n,HashSet<Object> list,int reason) {
    	if(n.isDelete) {
    		n.addVisitRef();
    	} else {
    		switch(reason) {
    		    case IndexUtil.SEARCH_REASON_DEL:
    		    	list.add(n.dataCell);
    		    	break;
    		    	
    		    case IndexUtil.SEARCH_REASON_QUERY:
    		    	list.add(n.dataCell.obj);
    		    	break;
    		}
    	}
    	
    	if(n.isNeedRealDelete()) {
    		mDaemon.addRemoveNode(n);
    	}
    	
	    if(n.equalList != null && n.equalList.size() != 0) {
			for(Object _n:n.equalList) {
				if(n.isDelete) {
					n.addVisitRef();
					if(n.isNeedRealDelete()) {
						mDaemon.addRemoveNode(n);
					}
					continue;
				}
				
				Node mNode = (Node)_n;
				switch(reason) {
    		        case IndexUtil.SEARCH_REASON_DEL:
    		    	    list.add(n.dataCell);
    		    	    break;
    		    	
    		        case IndexUtil.SEARCH_REASON_QUERY:
    		    	    list.add(SqlUtil.copyObj(n.dataCell.obj));
    		    	    break;
    		    }
			}
		}
    }
    
    private void markNodeToDelete(Node n) {
    	
    	StorageNozzle.deleteDataDirectly(className, n.dataCell);
    	n.markDelete();
    	
    	if(n.equalList != null && n.equalList.size() != 0) {
    		for(Object obj:n.equalList) {
    			Node mNode = (Node)obj;
    			mNode.markDelete();
    			StorageNozzle.deleteDataDirectly(className, mNode.dataCell);
    		}
    	}
    }
    
    
    private void nodeToListByCondition(Node node,HashSet<Object> list,int action ,T ele,int reason) {
    	ArrayList<Node>stack = new ArrayList<Node>();
    	stack.add(node);
    	
    	while(stack.size() != 0) {
    		Node n = stack.remove(stack.size() - 1);
    		int cmp = n.data.compareTo(ele);
    		
    		switch(action) {
    		    case Action.SQL_ACTION_EQUAL:
    		    	if(cmp == 0) {
    		    		nodeToList(n,list,reason);
    		    		return;
    		    	} else if(cmp < 0) {
    		    		if(n.right != null) {
    		    		    stack.add(n.right);
    		    		}
    		    	} else {
    		    		if(n.left != null) {
    		    			stack.add(n.left);
    		    		}
    		    	}
    			    break;
    		    case Action.SQL_ACTION_LESS_THAN:
    		    case Action.SQL_ACTION_LESS_THAN_OR_EQUAL:
    		    	    if(cmp < 0) {
    		    	    	
    		    	    	if(n.left != null) {
    		    	    	    stack.add(n.left);
    		    	    	}
    		    	    	
    		    	    	if(n.right != null) {
    		    	    		stack.add(n.right);
    		    	    	}
    		    	    	
    		    	    	nodeToList(n,list,reason);
    		    	    } else if(cmp > 0) {
    		    	    	if(n.left != null) {
    		    	    		stack.add(n.left);
    		    	    	}
    		    	    } else if(cmp == 0) {
    		    	    	if(action == Action.SQL_ACTION_LESS_THAN) {
    		    	    	    if(n.left != null) {
    		    	    	        stack.add(n.left);
    		    	    	    }
    		    	    	}else if(action == Action.SQL_ACTION_LESS_THAN_OR_EQUAL) {
    		    	    		if(n.left != null) {
    		    	    	        stack.add(n.left);
    		    	    	    }
    		    	    		
    		    	    		nodeToList(n,list,reason);
    		    	    	}
    		    	    }
    		    	break;
    		    	
                case Action.SQL_ACTION_MORE_THAN:
                case Action.SQL_ACTION_MORE_THAN_OR_EQUAL:
                	if(cmp > 0) {
                		
		    	    	if(n.left != null) {
		    	    	    stack.add(n.left);
		    	    	}
		    	    	
		    	    	if(n.right != null) {
		    	    		stack.add(n.right);
		    	    	}

		    	    	nodeToList(n,list,reason);
                		
                	}else if(cmp < 0) {
                		if(n.right != null) {
		    	    		stack.add(n.right);
		    	    	}
                	}else if(cmp == 0) {
		    	    	if(action == Action.SQL_ACTION_MORE_THAN) {
		    	    	    if(n.right != null) {
		    	    	        stack.add(n.left);
		    	    	    }
		    	    	}else if(action == Action.SQL_ACTION_MORE_THAN_OR_EQUAL) {
		    	    		if(n.right != null) {
		    	    	        stack.add(n.left);
		    	    	    }
		    	    		
		    	    		nodeToList(n,list,reason);
		    	    		
		    	    	}
		    	    }
                	
    		    	break;
    		}
    	}
    }
    

    public List<Node> breadthFirst() {
        Queue<Node> queue = new ArrayDeque<Node>();
        List<Node> list = new ArrayList<Node>();
        if (root != null) {
            queue.offer(root);
        }
        while (!queue.isEmpty()) {
            list.add(queue.peek());
            Node p = queue.poll();
            if (p.left != null) {
                queue.offer(p.left);
            }
            if (p.right != null) {
                queue.offer(p.right);
            }
        }
        return list;
    }

    private void fixAfterInsertion(Node x) {
        x.color = RED;
        while (x != null && x != root && x.parent.color == RED) {
            if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {
                Node y = rightOf(parentOf(parentOf(x)));
                if (colorOf(y) == RED) {
                    setColor(parentOf(x), BLACK);
                    setColor(y, BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    x = parentOf(parentOf(x));
                }
                else {
                    if (x == rightOf(parentOf(x))) {
                        x = parentOf(x);
                        rotateLeft(x);
                    }
                    setColor(parentOf(x), BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    rotateRight(parentOf(parentOf(x)));
                }
            }
            else {
                Node y = leftOf(parentOf(parentOf(x)));
                if (colorOf(y) == RED) {
                    setColor(parentOf(x), BLACK);
                    setColor(y, BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    x = parentOf(parentOf(x));
                }
                else {
                    if (x == leftOf(parentOf(x))) {
                        x = parentOf(x);
                        rotateRight(x);
                    }
                    setColor(parentOf(x), BLACK);
                    setColor(parentOf(parentOf(x)), RED);
                    rotateLeft(parentOf(parentOf(x)));
                }
            }
        }
        root.color = BLACK;
    }

    private void fixAfterDeletion(Node x) {
        while (x != root && colorOf(x) == BLACK) {
            if (x == leftOf(parentOf(x))) {
                Node sib = rightOf(parentOf(x));
                if (colorOf(sib) == RED) {
                    setColor(sib, BLACK);
                    setColor(parentOf(x), RED);
                    rotateLeft(parentOf(x));
                    sib = rightOf(parentOf(x));
                }
                if (colorOf(leftOf(sib)) == BLACK
                        && colorOf(rightOf(sib)) == BLACK) {
                    setColor(sib, RED);
                    x = parentOf(x);
                } else {
                    if (colorOf(rightOf(sib)) == BLACK) {
                        setColor(leftOf(sib), BLACK);
                        setColor(sib, RED);
                        rotateRight(sib);
                        sib = rightOf(parentOf(x));
                    }
                    setColor(sib, colorOf(parentOf(x)));
                    setColor(parentOf(x), BLACK);
                    setColor(rightOf(sib), BLACK);
                    rotateLeft(parentOf(x));
                    x = root;
                }
            }
            else {
                Node sib = leftOf(parentOf(x));
                if (colorOf(sib) == RED) {
                    setColor(sib, BLACK);
                    setColor(parentOf(x), RED);
                    rotateRight(parentOf(x));
                    sib = leftOf(parentOf(x));
                }
                if (colorOf(rightOf(sib)) == BLACK
                        && colorOf(leftOf(sib)) == BLACK) {
                    setColor(sib, RED);
                    x = parentOf(x);
                } else {
                    if (colorOf(leftOf(sib)) == BLACK) {
                        setColor(rightOf(sib), BLACK);
                        setColor(sib, RED);
                        rotateLeft(sib);
                        sib = leftOf(parentOf(x));
                    }
                    setColor(sib, colorOf(parentOf(x)));
                    setColor(parentOf(x), BLACK);
                    setColor(leftOf(sib), BLACK);
                    rotateRight(parentOf(x));
                    x = root;
                }
            }
        }
        setColor(x, BLACK);
    }

    private boolean colorOf(Node p) {
        return (p == null ? BLACK : p.color);
    }

    private Node parentOf(Node p) {
        return (p == null ? null : p.parent);
    }

    private void setColor(Node p, boolean c) {
        if (p != null) {
            p.color = c;
        }
    }

    private Node leftOf(Node p) {
        return (p == null) ? null : p.left;
    }

    private Node rightOf(Node p) {
        return (p == null) ? null : p.right;
    }

    private void rotateLeft(Node p) {
        if (p != null) {
            Node r = p.right;
            Node q = r.left;
            p.right = q;
            if (q != null) {
                q.parent = p;
            }
            r.parent = p.parent;
            if (p.parent == null) {
                root = r;
            }
            else if (p.parent.left == p) {
                p.parent.left = r;
            } else {
                p.parent.right = r;
            }
            r.left = p;
            p.parent = r;
        }
    }

    
    private void rotateRight(Node p) {
        if (p != null) {
            Node l = p.left;
            Node q = l.right;
            p.left = q;
            if (q != null) {
                q.parent = p;
            }
            l.parent = p.parent;
            if (p.parent == null) {
                root = l;
            }
            else if (p.parent.right == p) {
                p.parent.right = l;
            } else {
                p.parent.left = l;
            }
            l.right = p;
            p.parent = l;
        }
    }

    public List<Node> inIterator() {
        return inIterator(root);
    }

    private List<Node> inIterator(Node node) {
        List<Node> list = new ArrayList<Node>();
        if (node.left != null) {
            list.addAll(inIterator(node.left));
        }
        list.add(node);
        if (node.right != null) {
            list.addAll(inIterator(node.right));
        }
        return list;
    }

}
