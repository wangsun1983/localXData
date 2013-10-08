package com.localxdata.sql;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.localxdata.index.IndexTree;
import com.localxdata.index.IndexUtil;
import com.localxdata.index.Node;
import com.localxdata.storage.DataCellList;
import com.localxdata.struct.DataCell;
import com.localxdata.util.LogUtil;
import com.localxdata.util.PraseParamUtil;
import com.localxdata.util.PraseSqlUtil;
import com.localxdata.util.PraseSqlUtil.Action;
import com.localxdata.util.PraseSqlUtil.ActionTreeNode;
import com.localxdata.util.PraseSqlUtil.CombineAction;
import com.localxdata.util.PraseSqlUtil.ComputeAction;

public class SqlUtil {
     
	public static final String TAG = "SqlUtil";
	
    public static boolean compareField(Object obj1,Object obj2,Field f1,Field f2,int action) {
        
        boolean result = false;
        
        String type1 = f1.getType().getName();
        String type2 = f2.getType().getName();
        
        if(!type1.equals(type2)) {
            return false;
        }
        
        try {
            String compare2 = String.valueOf(f2.get(obj2));
            
            return compareData(obj1,compare2,f1,
                        PraseParamUtil.PraseObjectType(type1),
                        action);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        }
        
        return result;
    }
    
    
    public static boolean compareData(Object obj,String compareValue,Field field,
            int datatype,int actiontype) throws IllegalArgumentException, IllegalAccessException, SecurityException, NoSuchFieldException {
        switch(datatype) {
            case Action.DATA_TYPE_STRING:
                String stringValue = (String)field.get(obj);
                return compareStringData(stringValue,compareValue,actiontype);
        
            case Action.DATA_TYPE_BOOLEAN:
                boolean booleanValue = field.getBoolean(obj);
                boolean compareBoolean = Boolean.valueOf(compareValue);
                return compareBooleanData(booleanValue,compareBoolean,actiontype);
                
            case Action.DATA_TYPE_FLOAT:
                float floatValue = field.getFloat(obj);
                float comparefloat = Float.valueOf(compareValue);
                return compareFloatData(floatValue,comparefloat,actiontype);
                
            case Action.DATA_TYPE_LONG:
                //according to the sql,we cannot judge whether the field type is
                // long or int,so we return long in getDataType(String str);
                switch(PraseParamUtil.PraseObjectType(field.getType().getName())) {
                    case PraseParamUtil.PRASE_TYPE_LONG:
                        long longValue = field.getLong(obj);
                        long comparelong = Long.valueOf(compareValue);
                        return compareLongData(longValue,comparelong,actiontype);
                        
                    case PraseParamUtil.PRASE_TYPE_INT:
                        int intValue = field.getInt(obj);
                        int compareInt = Integer.valueOf(compareValue);
                        return compareIntData(intValue,compareInt,actiontype);
                }
            case Action.DATA_TYPE_TYPE_INT:
                int intValue = field.getInt(obj);
                int compareInt = Integer.valueOf(compareValue);
                if(intValue == compareInt) {
                    return true;
                }
                break;
                
            case Action.DATA_TYPE_ELEMENT:
                    switch(PraseParamUtil.PraseObjectType(field.getType().getName())) {
                        case PraseParamUtil.PRASE_TYPE_LONG:
                            long longElement = obj.getClass().getField(compareValue).getLong(compareValue);
                            long compareLongElement = field.getLong(obj);
                            return compareLongData(longElement,compareLongElement,actiontype);
                            
                        case PraseParamUtil.PRASE_TYPE_FLOAT:
                            float floatElement = obj.getClass().getField(compareValue).getFloat(compareValue);
                            float comparefloatElement = field.getFloat(obj);
                            return compareFloatData(floatElement,comparefloatElement,actiontype);
                            
                        case PraseParamUtil.PRASE_TYPE_INT:
                            int intElement = obj.getClass().getField(compareValue).getInt(compareValue);
                            int compareintElement = field.getInt(obj);
                            return compareIntData(intElement,compareintElement,actiontype);
                            
                        case PraseParamUtil.PRASE_TYPE_BOOLEAN:
                            boolean booleanElement = obj.getClass().getField(compareValue).getBoolean(compareValue);
                            boolean comparebooleanElement = field.getBoolean(obj);
                            return compareBooleanData(booleanElement,comparebooleanElement,actiontype);
                            
                        case PraseParamUtil.PRASE_TYPE_STRING:
                            String stringElement = (String)obj.getClass().getField(compareValue).get(compareValue);
                            String comparestringElement = (String)field.get(obj);
                            return compareStringData(stringElement,comparestringElement,actiontype); 
                    }
                break;
                
            default :
                //TODO     
                break;
        }
        
        return false;
    }
    
    public static boolean compareStringData(String fieldValue,String compareValue,int action) {
        
        switch(action) {
            case Action.SQL_ACTION_EQUAL :
                if(fieldValue.equals(compareValue)) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_NOT_EQUAL :
                if(fieldValue.equals(compareValue)) {
                    return false;
                }
                return true;
                
            case Action.SQL_ACTION_LIKE:
                return compareLikeData(fieldValue,compareValue);
        }
        return false;
    }
    
    //like pattern should be initialize only once!!!
    //TODO
    public static boolean compareLikeData(String fieldValue,String compareValue) {
                
        Pattern pattern = Pattern.compile(compareValue);
        Matcher matcher = pattern.matcher(fieldValue);
        
        if (matcher.find()){
            return true;
        }
        
        return false;
        
    }
    
    public static boolean compareBooleanData(Boolean fieldValue,Boolean compareValue,int action) {
        
        switch(action) {
            case Action.SQL_ACTION_EQUAL :
                if(fieldValue.equals(compareValue)) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_NOT_EQUAL :
                if(fieldValue.equals(compareValue)) {
                    return false;
                }
                return true;
        }
        return false;
    }
    
    public static boolean compareLongData(Long fieldValue,Long compareValue,int action) {
        switch(action) {
            case Action.SQL_ACTION_EQUAL :
                if(fieldValue == compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_NOT_EQUAL :
                if(fieldValue == compareValue) {
                    return false;
                }
                return true;
                
            case Action.SQL_ACTION_LESS_THAN :
                if(fieldValue < compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_MORE_THAN :
                if(fieldValue > compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_LESS_THAN_OR_EQUAL :
                if(fieldValue <= compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_MORE_THAN_OR_EQUAL :
                if(fieldValue >= compareValue) {
                    return true;
                }
                return false;
        }
        return false;
    }
    
    public static boolean compareFloatData(float fieldValue,float compareValue,int action) {
        switch(action) {
            case Action.SQL_ACTION_EQUAL :
                if(fieldValue == compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_NOT_EQUAL :
                if(fieldValue == compareValue) {
                    return false;
                }
                return true;
                
            case Action.SQL_ACTION_LESS_THAN :
                if(fieldValue < compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_MORE_THAN :
                if(fieldValue > compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_LESS_THAN_OR_EQUAL :
                if(fieldValue <= compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_MORE_THAN_OR_EQUAL :
                if(fieldValue >= compareValue) {
                    return true;
                }
                return false;
        }
        return false;
    }
    
    public static boolean compareIntData(int fieldValue,int compareValue,int action) {
        switch(action) {
            case Action.SQL_ACTION_EQUAL :
                if(fieldValue == compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_NOT_EQUAL :
                if(fieldValue == compareValue) {
                    return false;
                }
                return true;
                
            case Action.SQL_ACTION_LESS_THAN :
                if(fieldValue < compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_MORE_THAN :
                if(fieldValue > compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_LESS_THAN_OR_EQUAL :
                if(fieldValue <= compareValue) {
                    return true;
                }
                return false;
                
            case Action.SQL_ACTION_MORE_THAN_OR_EQUAL :
                if(fieldValue >= compareValue) {
                    return true;
                }
                return false;
        }
        return false;
    }
    
    public static Object copyObj(Object obj) {
        Object retdata = null;

        try {
            Class clazz = Class.forName(obj.getClass().getName());
            Constructor[] constructorList = clazz.getDeclaredConstructors();
            Constructor constructor = constructorList[0];
            constructor.setAccessible(true);
            Object membet = constructor.newInstance();
            
            Field[] fieldlist = obj.getClass().getDeclaredFields();
            
            for(Field field:fieldlist) {
                switch (PraseParamUtil.PraseObjectType(field.getType().getName())) {
                    case PraseParamUtil.PRASE_TYPE_INT:
                        Field intField = membet.getClass().getField(field.getName());
                        intField.setInt(membet, field.getInt(obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_BOOLEAN:
                        Field booleanField = membet.getClass().getField(field.getName());
                        booleanField.setBoolean(membet, field.getBoolean(obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_FLOAT:
                        Field floatField = membet.getClass().getField(field.getName());
                        floatField.setFloat(membet, field.getFloat(obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_LONG:
                        Field longField = membet.getClass().getField(field.getName());
                        longField.setLong(membet, field.getLong(obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_STRING:
                        Field stringField = membet.getClass().getField(field.getName());
                        stringField.set(membet, field.get(obj));
                        break;
                }
            }
            
            return membet;
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InstantiationException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SecurityException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (NoSuchFieldException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        return retdata;
    }
    
    public static void copyField(Field destField,Field srcField,Object destObject,Object srcObject) throws IllegalArgumentException, IllegalAccessException {
        destField.setAccessible(true);
        srcField.setAccessible(true);
        
        switch(PraseParamUtil.PraseObjectType(destField.getType().getName())) {
            case PraseParamUtil.PRASE_TYPE_INT:
                destField.setInt(destObject, srcField.getInt(srcObject));
                break;

            case PraseParamUtil.PRASE_TYPE_BOOLEAN:
                destField.setBoolean(destObject, srcField.getBoolean(srcObject));
                break;

            case PraseParamUtil.PRASE_TYPE_FLOAT:
                destField.setFloat(destObject, srcField.getFloat(srcObject));
                break;

            case PraseParamUtil.PRASE_TYPE_LONG:
                destField.setLong(destObject, srcField.getLong(srcObject));
                break;

            case PraseParamUtil.PRASE_TYPE_STRING:
                destField.set(destObject, srcField.get(srcObject));
                break;
        }        
    }
    
    //wangsl
    //fast search action start
    public static boolean checkDataByTree(Object obj,ActionTreeNode treeNode) {
        //we check right node first
        
        if(treeNode.leftChild != null && treeNode.rightChild != null) {
            boolean rightChildResult = checkDataByTree(obj,treeNode.rightChild);
        
            if(treeNode.action.mAction == Action.SQL_ACTION_AND && !rightChildResult) {
                return false;
            }
        
            if(treeNode.action.mAction == Action.SQL_ACTION_OR && rightChildResult) {
                return true;
            }
            
            boolean leftChildResult = checkDataByTree(obj,treeNode.leftChild);
            
            switch(treeNode.action.mAction) {
            case Action.SQL_ACTION_AND:
                return leftChildResult && rightChildResult;
            
            case Action.SQL_ACTION_OR:
                return leftChildResult || rightChildResult;
            }
        }
        
        ComputeAction computeAction;
        CombineAction combineAction;
        Action action = treeNode.action;
        
        if(action instanceof ComputeAction ) {
            computeAction = (ComputeAction)action;
            
            Field field = null;
            try {
                field = obj.getClass().getField(computeAction.mFieldName);
                field.setAccessible(true);
                boolean result = compareData(obj,computeAction.mData,field,computeAction.mDataType,computeAction.mAction);
                return result;
            } catch (SecurityException e) {
                e.printStackTrace();
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        
        return false;
    }
    
    public static boolean canUseIndex(ArrayList<Action>actionList,String tableName) {
    	ArrayList<Boolean> canUseIndexList = new ArrayList<Boolean>();
    	
    	for(Action action:actionList) {
    		if ((action instanceof PraseSqlUtil.ComputeAction)) {
        		PraseSqlUtil.ComputeAction computeAct = (PraseSqlUtil.ComputeAction) action;
        		
        		//TODO 
        		if(computeAct.mAction == Action.SQL_ACTION_NOT_EQUAL) {
        			return false;
        		}
        		//
        		
        		if(computeAct.mDataType == Action.DATA_TYPE_TYPE_INT
        		   ||computeAct.mDataType == Action.DATA_TYPE_FLOAT
                   ||computeAct.mDataType == Action.DATA_TYPE_LONG ) {
        		   boolean isExist = IndexUtil.getInstance().isIndexExist(tableName,
                            computeAct.mFieldName);
        		   if(!isExist) {
        			   canUseIndexList.add(false);
        		   }else {
        			   canUseIndexList.add(true);
        		   }
        		} else {
        			canUseIndexList.add(false);
        		}
        	} else if(action instanceof PraseSqlUtil.CombineAction) {
        		int size = canUseIndexList.size();
        		
        		PraseSqlUtil.CombineAction combineAction = (PraseSqlUtil.CombineAction) action;
        		Boolean canUseResult1 = canUseIndexList.get(size - 1);
        		Boolean canUseResult2 = canUseIndexList.get(size - 2);
        		
        		if(combineAction.mAction == Action.SQL_ACTION_OR) {
            		if(canUseResult1 || canUseResult2) {
            			canUseIndexList.remove(canUseResult1);
            			canUseIndexList.remove(canUseResult2);
            			canUseIndexList.add(true);
            			continue;
            		}
            		
            		return false;
        		} else {
            		if(canUseResult1&&canUseResult2) {
            			canUseIndexList.remove(canUseResult1);
            			canUseIndexList.remove(canUseResult2);
            			canUseIndexList.add(true);
                        continue;
            		}
            		
            		return false;
        		}
        	}
    	}
    	return true;
    }
    
    public static ArrayList<Object> checkDataByIndex(DataCellList datalist,String className,ArrayList<Action>actionList) {
    	//HashSet<Object> result = new HashSet<Object>();
    	
    	ArrayList<Object>predictResult = new ArrayList<Object>();
    	
    	for(Action action:actionList) {
        	if ((action instanceof PraseSqlUtil.ComputeAction)) {
        		PraseSqlUtil.ComputeAction computeAct = (PraseSqlUtil.ComputeAction) action;
        		if(computeAct.mDataType == Action.DATA_TYPE_TYPE_INT
        		   ||computeAct.mDataType == Action.DATA_TYPE_FLOAT
                   ||computeAct.mDataType == Action.DATA_TYPE_LONG) {
        			
        		   //IndexTree index = IndexUtil.getInstance().getIndexTree(className,
                   //         computeAct.mFieldName);
        		   boolean isExist = IndexUtil.getInstance().isIndexExist(className,
                              computeAct.mFieldName);
        		   
        		   if(isExist) {
        			   IndexTree index = IndexUtil.getInstance().getIndexTree(className,
        	                            computeAct.mFieldName);
        			   
        			   Node searchResult = IndexUtil.getInstance().searchNode(index,
                               computeAct.mAction,
                               Integer.valueOf(computeAct.mData));
        			   predictResult.add(searchResult);
        		   } else {
        			   predictResult.add(action);   
        		   }
        		}else {
        			predictResult.add(action);
        		}
        	}else if(action instanceof PraseSqlUtil.CombineAction) {
        		int size = predictResult.size();
        		
        		Object obj1 = predictResult.remove(size - 1);
        		Object obj2 = predictResult.remove(size - 2);
        		
         		if(obj1 instanceof Node && obj2 instanceof ComputeAction) {
         			predictResult.add(checkDataByIndex_1(
         					(Node)obj1,
         					(ComputeAction)obj2));
         			continue;
         		} else if(obj2 instanceof Node && obj1 instanceof ComputeAction) {
         			predictResult.add(checkDataByIndex_1(
         					(Node)obj2,
         					(ComputeAction)obj1));
         			continue;
         		} else if(obj1 instanceof Node && obj2 instanceof Node) {
         			predictResult.add(checkDataByIndex_2(
         					(Node)obj1,
         					(Node)obj2,
         					action.mAction));
         			continue;
         		} else if(obj1 instanceof Node && obj2 instanceof HashSet) {
         			predictResult.add(checkDataByIndex_3(
         					(Node)obj1,
         					(HashSet<Object>)obj2,
         					action.mAction));
         			continue;
         		} else if(obj2 instanceof Node && obj1 instanceof HashSet) {
         			predictResult.add(checkDataByIndex_3(
         					(Node)obj2,
         					(HashSet)obj1,
         					action.mAction));
         			continue;
         		} else if(obj1 instanceof HashSet && obj2 instanceof HashSet) {
         			predictResult.add(checkDataByIndex_4(
         					(HashSet<Object>)obj1,
         					(HashSet<Object>)obj2,
         					action.mAction));
         			continue;
         		}
        		
         		LogUtil.e(TAG, "checkDataByIndex no operation!!!");
        	}
        	
        }
    	
    	Object result = predictResult.get(0);
    	
    	if(result instanceof Node) {
    		HashSet<Object>list = new HashSet<Object>();
    		IndexUtil.getInstance().changeIndexToList(list, (Node)result);
    		result = list;
    	}
    	
    	return  new ArrayList<Object>((HashSet<Object>)result);
    }
    
    private static HashSet<Object> checkDataByIndex_1(Node node,ComputeAction computeAction){
    	HashSet<Object> list = new HashSet<Object>();
    	IndexUtil.getInstance().changeIndexToList(list, node);
    	
    	HashSet<Object>removeList = new HashSet<Object>();
    	
    	for(Object obj:list) {
    		
            Field field = null;
            boolean result = false;
            try {
                field = obj.getClass().getField(computeAction.mFieldName);
                field.setAccessible(true);
                result = compareData(obj,computeAction.mData,field,computeAction.mDataType,computeAction.mAction);
            } catch (SecurityException e) {
                e.printStackTrace();
            } catch (NoSuchFieldException e) {
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            
            if(!result) {
            	removeList.add(obj);
            }
    	}
    	
    	if(removeList != null||removeList.size() != 0) {
    	    list.removeAll(removeList);
    	}
    	
    	return list;
    }
    
    private static HashSet<Object> checkDataByIndex_2(Node node1,Node node2,int action){
    	HashSet<Object> list1 = new HashSet<Object>();
    	IndexUtil.getInstance().changeIndexToList(list1, node1);
    	
    	HashSet<Object> list2 = new HashSet<Object>();
    	IndexUtil.getInstance().changeIndexToList(list2, node2);
    	
    	return checkDataByIndex_4(list1,list2,action);
    }
    
    private static HashSet<Object> checkDataByIndex_3(Node node1,HashSet<Object> list2,int action){
    	HashSet<Object> list1 = new HashSet<Object>();
    	IndexUtil.getInstance().changeIndexToList(list1, node1);

    	return checkDataByIndex_4(list1,list2,action);
    }
    
    
    private static HashSet<Object> checkDataByIndex_4(HashSet<Object>list1,HashSet<Object>list2,int action){
    	switch(action) {
	        case Action.SQL_ACTION_OR:
	    	    list1.addAll(list2);
	    	    break;
	    	
	        case Action.SQL_ACTION_AND:
	    	    list1.retainAll(list2);
	    	    break;
	    	
	        default:
	    	    return null;
	    }
	
	    return list1;
    }
    
    public static boolean checkMultiJointByTree(HashMap<String,Object>checkdata,ActionTreeNode treeNode) {
        //we check right node first
        
        if(treeNode.leftChild != null && treeNode.rightChild != null) {
            boolean rightChildResult = checkDataByTree(checkdata,treeNode.rightChild);
        
            if(treeNode.action.mAction == Action.SQL_ACTION_AND && !rightChildResult) {
                return false;
            }
        
            if(treeNode.action.mAction == Action.SQL_ACTION_OR && rightChildResult) {
                return true;
            }
            
            boolean leftChildResult = checkDataByTree(checkdata,treeNode.leftChild);
            
            switch(treeNode.action.mAction) {
            case Action.SQL_ACTION_AND:
                return leftChildResult && rightChildResult;
            
            case Action.SQL_ACTION_OR:
                return leftChildResult || rightChildResult;
            }
        }
        
        ComputeAction computeAction;
        CombineAction combineAction;
        Action action = treeNode.action;
        
        if(action instanceof ComputeAction ) {
            computeAction = (ComputeAction)action;
            
            try {
            	//we should get data from HashMap<String,obj>
            	//1.getComputeAction data & fieldName
            	boolean result = true;
            	
            	//1.getFirst Join Args (com.test.student.id == com.test.subject.id)
            	//=>(first == second)    
            	String firstL[] = changeMultiFieldToTableAndId(computeAction.mFieldName);
            	String first_tableName_Field = firstL[MULTI_CLASS_FIELD];
            	String first_memberName_Field = firstL[MULTI_MEMBER_FIELD];
            	
            	Object firstObj = checkdata.get(first_tableName_Field);
				Field firstfield = firstObj.getClass().getField(first_memberName_Field);
            	
            	//2.getSecond Join Args
            	String secondL[] = changeMultiFieldToTableAndId(computeAction.mFieldName);
            	String second_tableName_Field = secondL[MULTI_CLASS_FIELD];
            	String second_memberName_Field = secondL[MULTI_MEMBER_FIELD];
            	
            	Object secondObj = checkdata.get(first_tableName_Field);
				Field secondfield = firstObj.getClass().getField(first_memberName_Field);
                //field = obj.getClass().getField(computeAction.mFieldName);
                
                //field.setAccessible(true);
                result = compareField(firstObj,secondObj,firstfield,secondfield,Action.SQL_ACTION_EQUAL);
                
                return result;
            } catch (SecurityException e) {
                e.printStackTrace();
            } catch (NoSuchFieldException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        
        return false;
    }
    
    public static boolean checkMultiDataByTree(HashMap<String,Object>checkdata,ActionTreeNode treeNode) {
    	if(treeNode.leftChild != null && treeNode.rightChild != null) {
            boolean rightChildResult = checkMultiDataByTree(checkdata,treeNode.rightChild);
        
            if(treeNode.action.mAction == Action.SQL_ACTION_AND && !rightChildResult) {
                return false;
            }
        
            if(treeNode.action.mAction == Action.SQL_ACTION_OR && rightChildResult) {
                return true;
            }
            
            boolean leftChildResult = checkMultiDataByTree(checkdata,treeNode.leftChild);
            
            switch(treeNode.action.mAction) {
            case Action.SQL_ACTION_AND:
                return leftChildResult && rightChildResult;
            
            case Action.SQL_ACTION_OR:
                return leftChildResult || rightChildResult;
            }
        }
        
        ComputeAction computeAction;
        CombineAction combineAction;
        Action action = treeNode.action;
        
        if(action instanceof ComputeAction ) {
            computeAction = (ComputeAction)action;
            
            //Because we can user com.test.student.name != com.test.subject.name
            //So,we must distinguish data and field.......
            String firstField[] = changeMultiFieldToTableAndId(computeAction.mData);
            Object firstObj = null;
            Field firstfield = null;
            
            if(firstField[MULTI_CLASS_FIELD].length() != 0 && firstField[MULTI_MEMBER_FIELD].length() != 0) {
            	//this maybe com.test.student.name or "hello world. this is wangsl....."
            	firstObj = checkdata.get(firstField[MULTI_CLASS_FIELD]);
            	
            	if(firstObj != null) {
            		try {
						firstfield = firstObj.getClass().getField(firstField[MULTI_MEMBER_FIELD]);
					} catch (SecurityException e) {
						//e.printStackTrace();
					} catch (NoSuchFieldException e) {
						//e.printStackTrace();
					}
            	}
            }
            
            
            String secondField[] = changeMultiFieldToTableAndId(computeAction.mFieldName);
            Object secondObj = null;
            Field secondfield = null;
            
            if(secondField[MULTI_CLASS_FIELD].length() != 0 && secondField[MULTI_MEMBER_FIELD].length() != 0) {
            	//this maybe com.test.student.name or "hello world. this is wangsl....."
            	secondObj = checkdata.get(secondField[MULTI_CLASS_FIELD]);
            	
            	if(secondObj != null) {
            		try {
            			secondfield = secondObj.getClass().getField(secondField[MULTI_MEMBER_FIELD]);
					} catch (SecurityException e) {
						e.printStackTrace();
					} catch (NoSuchFieldException e) {
						e.printStackTrace();
					}
            	}
            }
            
            //Both is field
            if(secondfield != null && firstfield != null) {
                return compareField(firstObj,secondObj,firstfield,secondfield,computeAction.mAction);
            } else if(secondfield != null && firstfield == null) {
            	String type = secondfield.getType().getName();
            	try {
					boolean result = compareData(secondObj,
							computeAction.mData,
							secondfield,PraseParamUtil.PraseObjectType(type),
							computeAction.mAction);
					return result;
				} catch (IllegalArgumentException e) {
					
					//e.printStackTrace();
				} catch (SecurityException e) {
					
					//e.printStackTrace();
				} catch (IllegalAccessException e) {
					
					//e.printStackTrace();
				} catch (NoSuchFieldException e) {
					//e.printStackTrace();
				}
            } else if(secondfield == null && firstfield != null) {
            	String type = firstfield.getType().getName();
            	try {
					boolean result = compareData(firstObj,
							computeAction.mFieldName,
							firstfield,PraseParamUtil.PraseObjectType(type),
							computeAction.mAction);
					return result;
				} catch (IllegalArgumentException e) {
					
					//e.printStackTrace();
				} catch (SecurityException e) {
					
					//e.printStackTrace();
				} catch (IllegalAccessException e) {
					
					//e.printStackTrace();
				} catch (NoSuchFieldException e) {
					//e.printStackTrace();
				}
            }
        }
        
        return false;
    }
    //fast search action end
    
    //wangsl
    
    public static boolean checkDataByAction(Object obj,ArrayList<Action> actionList) {
        
        ArrayList<Boolean>actionResult = new ArrayList<Boolean>();
        
        //Init HashMap to restor object field;
        HashMap<String,Field>mFildHashMap = new HashMap<String,Field>();
        Field[] fieldList = obj.getClass().getDeclaredFields();
        for(Field field:fieldList) {
            mFildHashMap.put(field.getName(), field);
        }
        
        for(Action action:actionList) {
            
            ComputeAction computeAction;
            CombineAction combineAction;
            
            if(action instanceof ComputeAction ) {
                computeAction = (ComputeAction)action;
                
                Field field = null;
                try {
                    field = obj.getClass().getField(computeAction.mFieldName);
                    field.setAccessible(true);
                    boolean result = compareData(obj,computeAction.mData,field,computeAction.mDataType,computeAction.mAction);
                    actionResult.add(result);
                } catch (SecurityException e) {
                    e.printStackTrace();
                    continue;
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                    continue;
                } catch (IllegalArgumentException e) {
                    e.printStackTrace();
                    continue;
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                    continue;
                }
                
            }else {
                combineAction = (CombineAction)action;
                boolean result1 = actionResult.remove(actionResult.size() - 1);
                boolean result2 = actionResult.remove(actionResult.size() - 1);
                
                switch(combineAction.mAction) {
                    case Action.SQL_ACTION_AND:
                        actionResult.add(result1&&result2);
                        break;
                    
                    case Action.SQL_ACTION_OR:
                        actionResult.add(result1||result2);
                        break;
                }
            }
        }
        return actionResult.get(0);
    }
    
    //Interfact for multiTable Start
    /**
     * 
     * com.test.student.id ->{com.test.student,id}
     * */
    public static int MULTI_CLASS_FIELD = 0;
    public static int MULTI_MEMBER_FIELD = 1;
    private static char CHAR_DOT = '.';
    
    public static String[] changeMultiFieldToTableAndId(String field) {
    	String []result = new String[2];
    	
    	char args[] = field.toCharArray();
    	int size = args.length - 1;
    	
    	for(;size > 0;size--) {
    	    if(args[size] == CHAR_DOT) {
    	    	break;
    	    }	
    	}
    	result[MULTI_CLASS_FIELD] = field.substring(0, size);
    	result[MULTI_MEMBER_FIELD] = field.substring(size + 1, args.length);
    	
    	return result;
    }
    
    public static ArrayList<Action> praseJoinArgs(String join) {
    	//TODO
    	return null;
    }
    //Interface for multiTable End
}
