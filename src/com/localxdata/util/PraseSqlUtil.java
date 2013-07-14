package com.localxdata.util;

import java.util.ArrayList;
import java.util.HashMap;


/**
 * 
 * .query(SampleClass.getClass().getname(),"((a>0 && b<100) || c>100)");
 * 
 *
 */
public class PraseSqlUtil {
    
    public static final int PRIORITY_HIGH_LEVEL_1 = 1;
    public static final int PRIORITY_HIGH_LEVEL_2 = 2;
    public static final int PRIORITY_HIGH_LEVEL_3 = 3;
    public static final int PRIORITY_HIGH_LEVEL_4 = 4;
   
    public static String OPERATION_AND = "&&";
    public static String OPERATION_OR = "||";
    public static String OPERATION_EQUAL = "==";
    public static String OPERATION_LEFT_BRACES = "("; 
    public static String OPERATION_RIGHT_BRACES = ")";
    public static String OPERATION_MORE_THAN = ">";
    public static String OPERATION_LESS_THAN = "<";
    public static String OPERATION_LESS_THAN_OR_EQUAL = "<=";
    public static String OPERATION_MORE_THAN_OR_EQUAL = ">=";
    public static String OPERATION_LIKE = "LIKE";
    public static String OPERATION_NOT_EQUAL = "!=";
    
    public static char MARK_SPOT = '.';
    public static char MARK_DOUBLE_QUOTATION = '"';
    public static char MARK_PERCENT = '%';
    public static char MARK_AND = '&';
    public static char MARK_OR = '|';
    public static char MARK_LEFT_BRACES = '(';
    public static char MARK_RIGHT_BRACES = ')';
    public static char MARK_MORE_THAN = '>';
    public static char MARK_LESS_THAN = '<';
    public static char MARK_EXCLAMATION = '!';
    public static char MARK_SINGLE_QUOTATION = '\'';
    public static char MARK_SPACE = ' ';
    public static char MARK_STAR = '*';
    
    public static char MARK_NUMBER_0 = '0';
    public static char MARK_NUMBER_9 = '9';
        
    public static final int PREDICT_IT_IS_STRING_DATA = 0;
    public static final int PREDICT_IT_IS_FLOAT_DATA = 1;
    public static final int PREDICT_IT_IS_LONG_DATA = 2;
    public static final int PREDICT_IT_IS_BOOLEAN_DATA = 3;
    public static final int PREDICT_IT_IS_LIKE_DATE = 4;
    public static final int PREDICT_IT_IS_FIELD = 5;
    
    public static final int PREDICT_IT_ERROR = -1;
    
    private HashMap <String,Integer>mPriorityMap;
    
    private static PraseSqlUtil instance;
    
    public static PraseSqlUtil getInstance() {
        if(instance == null) {
            instance = new PraseSqlUtil();
        }
        return instance;
    }
    
    private PraseSqlUtil() {
        //initialize the priority table
        mPriorityMap = new HashMap <String,Integer>();
        mPriorityMap.put(OPERATION_LEFT_BRACES,PRIORITY_HIGH_LEVEL_1);
        mPriorityMap.put(OPERATION_RIGHT_BRACES,PRIORITY_HIGH_LEVEL_1);

        mPriorityMap.put(OPERATION_EQUAL,PRIORITY_HIGH_LEVEL_2);
        mPriorityMap.put(OPERATION_MORE_THAN,PRIORITY_HIGH_LEVEL_2);
        mPriorityMap.put(OPERATION_LESS_THAN,PRIORITY_HIGH_LEVEL_2);
        mPriorityMap.put(OPERATION_LESS_THAN_OR_EQUAL,PRIORITY_HIGH_LEVEL_2);
        mPriorityMap.put(OPERATION_MORE_THAN_OR_EQUAL,PRIORITY_HIGH_LEVEL_2);
        mPriorityMap.put(OPERATION_LIKE,PRIORITY_HIGH_LEVEL_2);
        mPriorityMap.put(OPERATION_NOT_EQUAL,PRIORITY_HIGH_LEVEL_2);
        
        mPriorityMap.put(OPERATION_AND,PRIORITY_HIGH_LEVEL_3);
        mPriorityMap.put(OPERATION_OR,PRIORITY_HIGH_LEVEL_3);
    }
        
    public ArrayList<Action>changeSqlToAction(String sql) {
        
        ArrayList<Action>actionList = new ArrayList<Action>();
        ArrayList<String> rpn = getRPN(sql);
        
        ArrayList<String> stack = new ArrayList<String>();
        for(String s:rpn) {            
            if(s.equals(OPERATION_EQUAL)
              ||s.equals(OPERATION_MORE_THAN)
              ||s.equals(OPERATION_LESS_THAN)
              ||s.equals(OPERATION_LESS_THAN_OR_EQUAL)
              ||s.equals(OPERATION_MORE_THAN_OR_EQUAL)
              ||s.equals(OPERATION_NOT_EQUAL)
              ||s.equals(OPERATION_LIKE)) {
                String str1 = stack.remove(stack.size() - 1);
                String str2 = stack.remove(stack.size() - 1);
                int str1PredictResult = getPredictType(str1);
                int str2PredictResult = getPredictType(str2);

                ComputeAction computeAction = new ComputeAction();
                if(str1PredictResult == PREDICT_IT_IS_FIELD &&
                   str2PredictResult == PREDICT_IT_IS_FIELD) {
                    computeAction.mData = str1;
                    computeAction.mFieldName = str2;
                    computeAction.mDataType = Action.DATA_TYPE_ELEMENT;
                } else if(str1PredictResult != PREDICT_IT_IS_FIELD) {
                    computeAction.mData = str1;
                    computeAction.mFieldName = str2;
                    computeAction.mDataType = getDataType(str1PredictResult);
                    if(str1PredictResult == PREDICT_IT_IS_STRING_DATA
                    	||str1PredictResult == PREDICT_IT_IS_LIKE_DATE) {
                        String modifyStr = str1.substring(1, str1.length() - 1);
                        computeAction.mData = modifyStr;
                    }
                    
                    //if this is like we should rechange the string(%->.;*->.*)
                    //to adapte regex.....
                    if(str1PredictResult == PREDICT_IT_IS_LIKE_DATE) {
                    	computeAction.mData = changeForRegex(computeAction.mData);
                    }
                    
                } else {
                    computeAction.mData = str2;
                    computeAction.mFieldName = str1;
                    computeAction.mDataType = getDataType(str2PredictResult);
                    if(str2PredictResult == PREDICT_IT_IS_STRING_DATA || 
                    		str2PredictResult == PREDICT_IT_IS_LIKE_DATE) {
                        String modifyStr = str2.substring(1, str2.length() - 2);
                        computeAction.mData = modifyStr;
                    }
                    
                    if(str2PredictResult == PREDICT_IT_IS_LIKE_DATE) {
                    	computeAction.mData = changeForRegex(computeAction.mData);
                    }
                }
                
                //we should take care of the condition:
                //25 > a ->this also can use~~
                computeAction.mAction = getActionType(s);
                if(str2PredictResult != PREDICT_IT_IS_FIELD) {
                    switch(computeAction.mAction) {
                        case Action.SQL_ACTION_LESS_THAN:
                            computeAction.mAction = Action.SQL_ACTION_MORE_THAN;
                        break;
                        
                        case Action.SQL_ACTION_MORE_THAN:
                            computeAction.mAction = Action.SQL_ACTION_LESS_THAN;
                        break;
                        
                        case Action.SQL_ACTION_LESS_THAN_OR_EQUAL:
                            computeAction.mAction = Action.SQL_ACTION_MORE_THAN_OR_EQUAL;
                        break;
                        
                        case Action.SQL_ACTION_MORE_THAN_OR_EQUAL:
                            computeAction.mAction = Action.SQL_ACTION_LESS_THAN_OR_EQUAL;
                        break;
                    }
                }
                actionList.add(computeAction);
                continue;
            } else if(s.equals(OPERATION_AND) || s.equals(OPERATION_OR)) {
                if(stack.size() == 1) {
                    //just like (a > 1 && b) =>a 1 > b &&
                    ComputeAction computeAction = new ComputeAction();
                    computeAction.mFieldName = stack.remove(stack.size() - 1);
                    if(computeAction.mFieldName.contains(String.valueOf(MARK_EXCLAMATION))) {
                        computeAction.mData = String.valueOf(false);
                    } else {
                        computeAction.mData = String.valueOf(true);
                    }
                    computeAction.mDataType = Action.DATA_TYPE_BOOLEAN;
                    
                    actionList.add(computeAction);
                }
                
                CombineAction combineAction = new CombineAction();
                
                combineAction.actionId1 = actionList.size() - 1;
                combineAction.actionId2 = actionList.size() - 2;
                combineAction.mAction = getActionType(s);
                actionList.add(combineAction);
                continue;
            }
            
            stack.add(s);
        }
        
        return actionList;
    }
    
    private int getActionType(String str) {
        if(str.equals(OPERATION_EQUAL)) {
            return Action.SQL_ACTION_EQUAL;
        }
        
        if(str.equals(OPERATION_OR)) {
            return Action.SQL_ACTION_OR;
        }
        
        if(str.equals(OPERATION_AND)) {
            return Action.SQL_ACTION_AND;
        }
        
        if(str.equals(OPERATION_MORE_THAN)) {
            return Action.SQL_ACTION_MORE_THAN;
        }
        
        if(str.equals(OPERATION_LESS_THAN)) {
            return Action.SQL_ACTION_LESS_THAN;
        }
        
        if(str.equals(OPERATION_LESS_THAN_OR_EQUAL)) {
            return Action.SQL_ACTION_LESS_THAN_OR_EQUAL;
        }
        
        if(str.equals(OPERATION_MORE_THAN_OR_EQUAL)) {
            return Action.SQL_ACTION_MORE_THAN_OR_EQUAL;
        }
        
        if(str.equals(OPERATION_NOT_EQUAL)) {
            return Action.SQL_ACTION_NOT_EQUAL;
        }
        
        if(str.equals(OPERATION_LIKE)) {
            return Action.SQL_ACTION_LIKE;
        }
        
        return Action.SQL_ACTION_IDLE;
    }
    
    private int getDataType(int predictType) {
        
        switch(predictType) {
            case PREDICT_IT_IS_STRING_DATA:
                return Action.DATA_TYPE_STRING;
                
            case PREDICT_IT_IS_FLOAT_DATA:
                return Action.DATA_TYPE_FLOAT;
                
            case PREDICT_IT_IS_LONG_DATA:
                return Action.DATA_TYPE_LONG;
                
            case PREDICT_IT_IS_BOOLEAN_DATA:
                return Action.DATA_TYPE_BOOLEAN;
                
            case PREDICT_IT_IS_LIKE_DATE:
                return Action.DATA_TYPE_STRING;
        }
        return Action.DATA_TYPE_ERROR;
    }
    
    private int getPredictType(String str) {
        char strList[] = str.toCharArray();

        //String data
        if(strList[0] == MARK_DOUBLE_QUOTATION && 
                strList[str.length() - 1] == MARK_DOUBLE_QUOTATION) {
            return PREDICT_IT_IS_STRING_DATA;
        }

        //Like data
        if(strList[0] == MARK_SINGLE_QUOTATION && 
                strList[str.length() - 1] == MARK_SINGLE_QUOTATION) {
            return PREDICT_IT_IS_LIKE_DATE;
        }
        
        //number data
        boolean isNumber = true;
        boolean isFloat = false;
        for(int i = 0;i<strList.length;i++) {
            if(isNumber(strList[i])) {
               continue; 
            } else if((strList[i] == MARK_SPOT 
                    && isNumber(strList[i - 1]) 
                    && isNumber(strList[i + 1]))){
                isFloat = true;
            } else {
                isNumber = false;
                break;
            }
        }
        if(isNumber) {
            if(isFloat) {
                return PREDICT_IT_IS_FLOAT_DATA;
            } else {
            	//we cannot differentiate between long and int,
                //so we return long.
                return PREDICT_IT_IS_LONG_DATA;
            }
        }
        
        //boolean data
        if(str.equalsIgnoreCase(String.valueOf(true))||
                str.equalsIgnoreCase(String.valueOf(false))) {
            return PREDICT_IT_IS_BOOLEAN_DATA;
        }
        
        return PREDICT_IT_IS_FIELD;
    }
    
    
    private boolean isNumber(char c) {
        if(c < MARK_NUMBER_0 || c > MARK_NUMBER_9) {
            return false;
        }
        return true;
    }
    
    /**
     * We use Reverse Polish notation(RPN) to anylize the sql query.
     * @param query:sql query
     * @return
     */
    private ArrayList<String> getRPN(String query) {
        ArrayList<String> nodeList = new ArrayList<String>();
        ArrayList<String> operationList = new ArrayList<String>();
        
        //do anylize
        char queryList[] = query.replaceAll("\\s+", "").toCharArray();
        int record = 0;
        int i = 0;
        
        for(;i<queryList.length;i++) {
            char c = queryList[i];
            
            if(c == MARK_LEFT_BRACES) {
                operationList.add(String.valueOf(c));
                continue;
            }
            
            if(c == MARK_RIGHT_BRACES){
                for(int k = operationList.size() - 1;k >= 0;k--) {
                    String d = operationList.get(k);
                    if(!d.equals(OPERATION_LEFT_BRACES)) {
                        nodeList.add(d);
                        operationList.remove(d);
                    } else {
                        operationList.remove(d);
                        break;
                    }
                }
                if(record != 0) {
                    nodeList.add(String.valueOf(queryList, i - record, record));
                    record = 0;
                }   
                continue;
            }
            
            if((c == OPERATION_AND.charAt(0) && queryList[i+1] == OPERATION_AND.charAt(1))
                    ||(c == OPERATION_OR.charAt(0) && queryList[i+1] == OPERATION_OR.charAt(1))
                    ||(c == OPERATION_LESS_THAN_OR_EQUAL.charAt(0) && queryList[i+1] == OPERATION_LESS_THAN_OR_EQUAL.charAt(1))
                    ||(c == OPERATION_MORE_THAN_OR_EQUAL.charAt(0) && queryList[i+1] == OPERATION_MORE_THAN_OR_EQUAL.charAt(1))
                    ||(c == OPERATION_EQUAL.charAt(0) && queryList[i+1] == OPERATION_EQUAL.charAt(1))
                    ||(c == OPERATION_NOT_EQUAL.charAt(0) && queryList[i+1] == OPERATION_NOT_EQUAL.charAt(1))) {
                String operation = String.valueOf(queryList, i, 2);
                
                int operationNewPri = mPriorityMap.get(operation);
                
                if(operationList.size() > 0) {
                    String top = operationList.get(operationList.size() - 1);
                    int operationOldPri = mPriorityMap.get(top);
                
                    if((operationNewPri < operationOldPri) 
                            || (top.equals(OPERATION_LEFT_BRACES))) {
                        operationList.add(operation);
                    } else {
                        operationList.remove(top);
                        nodeList.add(top);
                        operationList.add(operation);
                    }
                } else {
                    operationList.add(operation);
                }
                
                if(record != 0) {
                    nodeList.add(String.valueOf(queryList, i - record, record));
                    record = 0;
                }
                i++;
                continue;
            }
            
            if(c == MARK_MORE_THAN || c == MARK_LESS_THAN) {
                String operation = String.valueOf(c);
                operationList.add(operation);
                if(record != 0) {
                    nodeList.add(String.valueOf(queryList, i - record, record));
                    record = 0;
                } 
                continue;
            }
            
            if(c == MARK_DOUBLE_QUOTATION) {
                int j = i + 1;
                for(;j < queryList.length;j++) {
                    if(queryList[j] == MARK_DOUBLE_QUOTATION){
                        break;
                    }
                }
                
                String node = String.valueOf(queryList,i,j - i + 1);
                nodeList.add(node);
                i = j;
                if(record != 0) {
                    nodeList.add(String.valueOf(queryList, i - record, record));
                    record = 0;
                }
                continue;
            }
            
            if(c == MARK_SINGLE_QUOTATION) {
                int j = i + 1;
                for(;j < queryList.length;j++) {
                    if(queryList[j] == MARK_SINGLE_QUOTATION){
                        break;
                    }
                }
                
                String node = String.valueOf(queryList,i,j - i + 1);
                nodeList.add(node);
                i = j;
                if(record != 0) {
                    nodeList.add(String.valueOf(queryList, i - record, record));
                    record = 0;
                }
                continue;
            }
            
            if(c >= MARK_NUMBER_0 && c <= MARK_NUMBER_9) {
            	//if there is A~Z/a~z before Number,this must be a member which is named like [public int data1]
                
            	if(record != 0) {
            		record++;
            		continue;
            	}
            	//wangsl
                int j = i + 1;
                for(;j < queryList.length;j++) {
                    if((queryList[j] < MARK_NUMBER_0 || queryList[j] > MARK_NUMBER_9)&&
                        queryList[j] != MARK_SPOT){
                        break;
                    }
                }
                
                String node = String.valueOf(queryList,i,j - i);
                nodeList.add(node);
                record = 0;
                i = j - 1;
                continue;
            }
            
            if(c == OPERATION_LIKE.charAt(0) 
               && queryList[i+1] == OPERATION_LIKE.charAt(1)
               && queryList[i+2] == OPERATION_LIKE.charAt(2)
               && queryList[i+3] == OPERATION_LIKE.charAt(3)) {
                
            	int position1 = 0;
            	int position2 = 0;
                //This maybe LIKE operation,
                //but we should take care this condition LIKE = 5 && name LIKE 'abc%'
                for(int j = i + 4;j < queryList.length; j++) {
                	if(queryList[j] == MARK_SPACE) {
                		continue;
                	}
                	else if(queryList[j] != MARK_SINGLE_QUOTATION) {
                		break;
                	}else {
                		position1 = j;
                		int k = j + 1;
                		for(;k < queryList.length;k++) {
                			if(queryList[k] == MARK_SINGLE_QUOTATION) {
                				position2 = k;
                				break;
                			}
                		}
                		
                		String operation = OPERATION_LIKE;
                        
                        int operationNewPri = mPriorityMap.get(operation);
                        
                        if(operationList.size() > 0) {
                            String top = operationList.get(operationList.size() - 1);
                            int operationOldPri = mPriorityMap.get(top);
                        
                            if((operationNewPri < operationOldPri) 
                                    || (top.equals(OPERATION_LEFT_BRACES))) {
                                operationList.add(operation);
                            } else {
                                operationList.remove(top);
                                nodeList.add(top);
                                operationList.add(operation);
                            }
                        } else {
                            operationList.add(operation);
                        }
                        
                        if(record != 0) {
                            nodeList.add(String.valueOf(queryList, i - record, record));
                            
                            //wangsl
                            nodeList.add(String.valueOf(queryList, position1, position2-position1 + 1));
                            //wangsl
                            record = 0;
                        }
                        i = i + 4 + (position2-position1);
                        break;
                	}
                }
                continue;
            }
            
            record++;
        }
        
        if(record != 0) {
            nodeList.add(String.valueOf(queryList, i - record, record));
        }
        
        //last out
        if(operationList.size() != 0) {
            for(int k = operationList.size() - 1;k >= 0;k--) {
                String d = operationList.get(k);
                nodeList.add(d);
            }
        }
        return nodeList;
    }
    
    //wangsl
    public ActionTreeNode changeSqlToActionTree(String sql) {
        //TODO
        return null;
    }
    //wangsl
    
    //fast search action start
    public ActionTreeNode changeActionListToTree(ArrayList<Action> actionList) {
        
        ArrayList<Action> list = new ArrayList(actionList);
        
        ActionTreeNode rootNode = new ActionTreeNode();
        
        ArrayList<ActionTreeNode> stack = new ArrayList<ActionTreeNode>();
        
        //if there is only one judgement
        if(list.size() == 1) {
            rootNode.action = list.get(0);
            return rootNode;
        }
        
        while(list.size() != 0) {
            Action action = list.remove(0);
            if(action.mAction == Action.SQL_ACTION_OR 
               ||action.mAction == Action.SQL_ACTION_AND) {
                if(stack.size() < 2) {
                    return null;
                }
                
                ActionTreeNode leftAction = stack.remove(stack.size()-1);
                ActionTreeNode rightAction = stack.remove(stack.size()-1);
                
                rootNode = new ActionTreeNode();
                rootNode.leftChild = leftAction;
                rootNode.rightChild = rightAction;
                rootNode.action = action;
                stack.add(rootNode);
            } else {
                ActionTreeNode node = new ActionTreeNode();
                node.action = action;
                stack.add(node);
            }
        }
        
        
        return rootNode;
    }
    //fast search action start

    private String changeForRegex(String data) {
    	
    	String compareValueRegx = data.replaceAll(
                String.valueOf(PraseSqlUtil.MARK_PERCENT), String.valueOf(PraseSqlUtil.MARK_SPOT));
        
    	//we should replace * to .x for regex
        //TODO this operation should be modified after making actionlist
        char []regxCharArray = compareValueRegx.toCharArray();
        
        char []regxNewCharArray = new char[regxCharArray.length * 2];
        int regxCount = 0;
        int regxCountNew = 0;
        for(char c:regxCharArray) {
        	if(c == PraseSqlUtil.MARK_STAR) {
        		regxNewCharArray[regxCountNew] = PraseSqlUtil.MARK_SPOT;
        		regxCountNew++;
        		regxNewCharArray[regxCountNew] = PraseSqlUtil.MARK_STAR;
        		regxCountNew++;
        		
        		regxCount++;
        	}else {
        		regxNewCharArray[regxCountNew] = regxCharArray[regxCount];
        		regxCountNew ++;
        		regxCount ++;
        	}
        }
        
        return String.valueOf(regxNewCharArray).substring(0, regxCountNew);
    }
    
    public class Action {
        public static final int SQL_ACTION_IDLE = 0;
        public static final int SQL_ACTION_EQUAL = 1;
        public static final int SQL_ACTION_NOT_EQUAL = 2;
        public static final int SQL_ACTION_MORE_THAN = 3;
        public static final int SQL_ACTION_LESS_THAN = 4;
        public static final int SQL_ACTION_MORE_THAN_OR_EQUAL = 5;
        public static final int SQL_ACTION_LESS_THAN_OR_EQUAL = 6;
        public static final int SQL_ACTION_AND = 7;
        public static final int SQL_ACTION_OR = 8;
        public static final int SQL_ACTION_LIKE = 9; //TODO
        
        public static final int DATA_TYPE_ERROR = -1;
        public static final int DATA_TYPE_STRING = PraseParamUtil.PRASE_TYPE_STRING;
        public static final int DATA_TYPE_TYPE_INT = PraseParamUtil.PRASE_TYPE_INT;
        public static final int DATA_TYPE_LONG = PraseParamUtil.PRASE_TYPE_LONG;
        public static final int DATA_TYPE_BOOLEAN = PraseParamUtil.PRASE_TYPE_BOOLEAN;
        public static final int DATA_TYPE_FLOAT = PraseParamUtil.PRASE_TYPE_FLOAT;
        public static final int DATA_TYPE_ELEMENT = PraseParamUtil.PRASE_TYPE_MAX + 1;
        
        public int mAction = SQL_ACTION_IDLE;
    }

    public class ComputeAction extends Action{
        public String mFieldName;
        public String mData;
        public int mDataType;
    }


    public class CombineAction extends Action{
        public int actionId1;
        public int actionId2;
    }
    
    //fast search action start
    public class ActionTreeNode {
        public ActionTreeNode fatherChild;
        public ActionTreeNode rightChild;
        public ActionTreeNode leftChild;
        public Action action;
    }
    //fast search action end
}

