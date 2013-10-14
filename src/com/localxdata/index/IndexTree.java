package com.localxdata.index;

import java.util.*;

import com.localxdata.storage.DataCellList;
import com.localxdata.struct.DataCell;
import com.localxdata.util.PraseSqlUtil.Action;

public class IndexTree<T extends Comparable> {
    //Define the color
    private static final boolean RED = Node.RED;
    private static final boolean BLACK = Node.BLACK;
    
    private static final int DELETE = 0;
    private static final int QUERY = 1;

    private Node root;

    public IndexTree() {
        root = null;
    }

    public void add(DataCell dataCell, T ele) {

        if (root == null) {
            root = new Node(dataCell, ele, null, null, null,null);
        } else {
            Node current = root;
            Node parent = null;
            int cmp = 0;

            do {
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

            Node newNode = new Node(dataCell, ele, parent, null, null,null);

            if (cmp > 0) {
                parent.right = newNode;
                fixAfterInsertion(newNode);
            } else if(cmp == 0) {
            	if(parent.equalList == null) {
            		parent.equalList = new ArrayList<Node>();
            	}
                parent.equalList.add(newNode);
            } else {
                parent.left = newNode;
                fixAfterInsertion(newNode);
            }
        }
    }

    
    public void remove(Node target) {

    	//if the equalist has the same value;
    	if(target.equalList != null && target.equalList.size() != 0) {
    		Node placement = (Node) target.equalList.remove(0);
    		placement.parent = target.parent;
            
            if (target.parent == null) {
                root = placement;
            }
        
            else if (target == target.parent.left) {
                target.parent.left = placement;
            }
        
            else {        
                target.parent.right = placement;
            }
            target.left = target.right = target.parent = null;

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
    }

    public Node getNode(DataCell datacell, T ele) {
        Node p = this.root;
        while (p != null) {
            int cmp = ele.compareTo(p.data);

            if (cmp < 0) {
                p = p.left;
            } else if (cmp > 0) {
                p = p.right;
            } else {
            	//TODO we may add write lock!!
            	for(Object obj:p.equalList) {
            		Node n = (Node)obj;
            		if (n.dataCell.obj == datacell.obj) {
                        return p;
                    }
            	}
            }
        }

        return null;
    }

    public HashSet<Object> getNode(int action,T ele) {
    	HashSet<Object> list = new HashSet<Object>();
    	nodeToListByCondition(this.root,list,action,ele,QUERY);
    	return list;
    }

    //public void remove(Node node,int action,Tele ele) {
    //	
    //}
    
    private void nodeToListByCondition(Node node,HashSet<Object> list,int action ,T ele,int DelOrQuery) {
    	ArrayList<Node>stack = new ArrayList<Node>();
    	stack.add(node);
    	
    	while(stack.size() != 0) {
    		Node n = stack.remove(stack.size() - 1);
    		int cmp = n.data.compareTo(ele);
    		
    		switch(action) {
    		    case Action.SQL_ACTION_EQUAL:
    		    	if(cmp == 0) {
    		    		if(DelOrQuery == QUERY) {
    		    		    list.add(n.dataCell.obj);
    		    		    if(n.equalList != null && n.equalList.size() != 0) {
        		    			for(Object _n:n.equalList) {
        		    				Node mNode = (Node)_n;
        		    				list.add(mNode.dataCell.obj);
        		    			}
        		    		}
    		    		} else if(DelOrQuery == DELETE) {
    		    			
    		    		} 

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
    		    	    	if(DelOrQuery == QUERY) {
    		    	    	    list.add(n.dataCell.obj);
    		    	    	
    		    	    	    if(n.equalList != null && n.equalList.size() != 0) {
    		    	    		    for(Object _n:n.equalList) {
        		    				    Node mNode = (Node)_n;
        		    				    list.add(mNode.dataCell.obj);
        		    			    }
    		    	    	    }
    		    	    	} else if(DelOrQuery == DELETE) {
    		    	    		n.setDelete();
    		    	    	}
    		    	    	
    		    	    	if(n.left != null) {
    		    	    	    stack.add(n.left);
    		    	    	}
    		    	    	
    		    	    	if(n.right != null) {
    		    	    		stack.add(n.right);
    		    	    	}
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
    		    	    		
    		    	    		list.add(n.dataCell.obj);
    		    	    		if(n.equalList != null && n.equalList.size() != 0) {
        		    	    		for(Object _n:n.equalList) {
            		    				Node mNode = (Node)_n;
            		    				list.add(mNode.dataCell.obj);
            		    			}
        		    	    	}
    		    	    	}
    		    	    }
    		    	break;
    		    	
                case Action.SQL_ACTION_MORE_THAN:
                case Action.SQL_ACTION_MORE_THAN_OR_EQUAL:
                	if(cmp > 0) {
                		list.add(n.dataCell.obj);
		    	    	
		    	    	if(n.equalList != null && n.equalList.size() != 0) {
		    	    		for(Object _n:n.equalList) {
    		    				Node mNode = (Node)_n;
    		    				list.add(mNode.dataCell.obj);
    		    			}
		    	    	}
		    	    	
		    	    	if(n.left != null) {
		    	    	    stack.add(n.left);
		    	    	}
		    	    	
		    	    	if(n.right != null) {
		    	    		stack.add(n.right);
		    	    	}
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
		    	    		
		    	    		list.add(n.dataCell.obj);
		    	    		if(n.equalList != null && n.equalList.size() != 0) {
    		    	    		for(Object _n:n.equalList) {
        		    				Node mNode = (Node)_n;
        		    				list.add(mNode.dataCell.obj);
        		    			}
    		    	    	}
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
