package com.localxdata.storage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import com.localxdata.config.ConfigNozzle;
import com.localxdata.struct.DataCell;
import com.localxdata.util.LogUtil;
import com.localxdata.util.XmlUtil;

/**
 * We should use btree to make db smaller. Because if we save 50K records in a
 * file, the io opreation will cost large memory and a lot of time.Also if we
 * only need to change one records in the 50K records,we need not to write all
 * the records to file again.we can write to the file which save the data.
 * */

class ModifyTable {
    public static final int STATE_IDLE = 0;
    public static final int STATE_UPDATE = 1;
    public static final int STATE_DEL = 2;
    public static final int STATE_INSERT = 3;

    String className;
    int state = STATE_IDLE;
    int startIndex = 0;
    int endIndex = 0;
    int position = 0;

    public ModifyTable(int state, String classname,int start,int end) {
        this.className = classname;
        this.state = state;
        this.startIndex = start;
        this.endIndex = end;
        this.position = -1;
    }
    
    public ModifyTable(int state,String classname,int pos) {
        this.className = classname;
        this.state = state;
        this.startIndex = -1;
        this.endIndex = -1;
        this.position = pos;
    }
}

public class SaveHandler extends Thread {

    public static int INTERVAL_SHORT = 1000;
    public static int INTERVAL_MIDIUM = 1000 * 10;
    public static int INTERVAL_LONG = 1000 * 60 * 15;
    
    private static final int FIND_RESULT_START_CELL = 0;
    private static final int FIND_RESULT_END_CELL = 1;

    private static final String DEBUG_TAG = "StoreSqlThread";

    private int mInterval = INTERVAL_MIDIUM;

    private XmlUtil mXmlUtilInstance;

    private static HashMap<String, DataCellList> mStoreMap;

    private ArrayList<ModifyTable> changedTable = new ArrayList<ModifyTable>();

    private boolean isNeedRun = true;

    private boolean isRun = false;

    private static SaveHandler instance;

    private SaveHandler() {
        mXmlUtilInstance = XmlUtil.getInstance();
    }

    public static synchronized SaveHandler getInstance() {
        if (instance == null) {
            instance = new SaveHandler();
            instance.start();
        }

        return instance;
    }

    public void init() {
        // TODO Nothing
    }

    public void setStoreDataBase(HashMap<String, DataCellList> map) {
        mStoreMap = map;
    }

    public void addInsertTable(String className,int pos) {
        synchronized (changedTable) {
            changedTable.add(new ModifyTable(ModifyTable.STATE_INSERT, className,pos));
        }
    }
    
    public void addInsertTable(String className,int start,int end) {
        synchronized (changedTable) {
            changedTable.add(new ModifyTable(ModifyTable.STATE_INSERT, className,start,end));
        }
    }

    public void addUpdateTable(String className,int pos) {
        synchronized (changedTable) {
            changedTable.add(new ModifyTable(ModifyTable.STATE_UPDATE, className,pos));
        }
    }
    
    public void addUpdateTable(String className,int start,int end) {
        synchronized (changedTable) {
            changedTable.add(new ModifyTable(ModifyTable.STATE_UPDATE, className,start,end));
        }
    }
    

    public void addDeleteTable(String className, int start, int end) {
        synchronized (changedTable) {
            changedTable.add(new ModifyTable(ModifyTable.STATE_DEL, className,start,end));
        }
    }
    
    public void addDeleteTable(String className, int pos) {
        synchronized (changedTable) {
            changedTable.add(new ModifyTable(ModifyTable.STATE_DEL, className,pos));
        }
    } 

    public void setInverval(int interval) {
        mInterval = interval;
    }

    public void stopThread() {
        isNeedRun = false;
    }

    public boolean isThreadRun() {
        return isRun;
    }

    public void run() {
        while (isNeedRun) {

            isRun = true;

            if (changedTable.size() == 0) {
                try {
                    // LogUtil.d(DEBUG_TAG,"sleep!!!");
                    Thread.sleep(mInterval);
                    continue;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            HashSet<String>list = null;
            synchronized (changedTable) {
            	list = changeTableExtract(changedTable);
                changedTable.clear();
            }
            
            if(list == null || list.size() == 0) {
            	continue;
            }
            
            for(String filename:list) {
            	if(TableControl.isTableExists(filename)) {
            		String className = mXmlUtilInstance.transformClassName(filename);//filename.substring(0,filename.lastIndexOf("_"));
            		int blockNum = mXmlUtilInstance.transformBlockNum(filename);
  		
            		DataCellList dataCellList = MemoryData.getDataList(className);
            		DataCell cellRange[] = findDataCellByBlocknum(dataCellList,Integer.valueOf(blockNum));
            		XmlUtil.getInstance().updateDataXml(className, 
            				                            dataCellList, 
            				                            cellRange[FIND_RESULT_START_CELL], 
            				                            cellRange[FIND_RESULT_END_CELL],
            				                            blockNum);
            	}else {
            		
            		String className = mXmlUtilInstance.transformClassName(filename);//filename.substring(0,filename.lastIndexOf("_"));
            		int blockNum = mXmlUtilInstance.transformBlockNum(filename);
            		
            		DataCellList dataCellList = MemoryData.getDataList(className);
            		
            		if(blockNum == 0) {
            		    XmlUtil.getInstance().createDataXml(className, dataCellList, Integer.valueOf(blockNum));
            		}else {
            			DataCell cellRange[] = findDataCellByBlocknum(dataCellList,Integer.valueOf(blockNum));
            			XmlUtil.getInstance().createDataXml(className, dataCellList, 
            					cellRange[FIND_RESULT_START_CELL], 
	                            cellRange[FIND_RESULT_END_CELL], 
	                            blockNum);
            		}
            		
            		int maxRecords = ConfigNozzle.getDataMaxFileRecord();
            		TableControl.addExactTable(className, blockNum*maxRecords, (blockNum+1)*maxRecords - 1,blockNum);
            	}
            }
           
        }

        isRun = false;
    }
    
    private DataCell[] findDataCellByBlocknum(DataCellList array,int blocknum) {
    	DataCell[]result = new DataCell[2];
    	
    	int size = array.size();
    	
    	int maxValue = (blocknum + 1)*ConfigNozzle.getDataMaxFileRecord() - 1;
    	int minValue = blocknum*ConfigNozzle.getDataMaxFileRecord();
    	
    	
    	int maxRange = (blocknum + 1)*ConfigNozzle.getDataMaxFileRecord();
    	int minRange = 0;
    	
    	if(maxRange > size) {
    		maxRange = size - 1;
    	}
    	
    	if(array.get(size - 1).getId() < minValue) {
    		return null;
    	}
    	
    	//find min
    	int startId = minRange;
    	int endId = maxRange;
    	
    	DataCell value = null;
    	while(startId <= endId) {
    		
    		int i = (startId+endId)/2;
    		
    		if(endId - startId == 1) {
    			if(array.get(startId).getId() >= minValue){
    				value = array.get(startId);
    			    break;
    			}
    			
    			if(array.get(endId).getId() >= minValue) {
    				value = array.get(endId);
    			}
    			break;
    		}
    		
    		DataCell c = array.get(i);
    		if(c.getId() == minValue) {
    			value = c;
    			break;
    		}else if(c.getId() < minValue) {
    			startId = i;
    		}else if(c.getId() > minValue) {
    			endId = i;
    			value = c;
    		}
    	}
    	result[FIND_RESULT_START_CELL] = value;
    	
    	//find max
    	startId = minRange;
    	endId = maxRange;
    	
    	value = null;
    	
    	while(startId <= endId) {
    		
    		int i = (startId+endId)/2;
    		
    		if(endId - startId == 1) {
    			if(array.get(endId).getId() <= maxValue){
    				value = array.get(endId);
    			    break;
    			}
    			
    			if(array.get(startId).getId() <= maxValue) {
    				value = array.get(startId);
    			}
    			break;
    		}
    		
    		DataCell c = array.get(i);
    		if(c.getId() == maxValue) {
    			value = c;
    			break;
    		}else if(c.getId() < maxValue) {
    			startId = i;
    			value = c;
    		}else if(c.getId() > minValue) {
    			endId = i;
    		}
    	}
    	result[FIND_RESULT_END_CELL] = value;
    	
    	return result;
    }
   
    private HashSet<String> changeTableExtract(ArrayList<ModifyTable> changedTable) {
    	
    	HashSet<String> list = new HashSet<String>();
    	
    	for(ModifyTable table:changedTable) {
    		if(table.position != -1) {
    			int blockNum = table.position/ConfigNozzle.getDataMaxFileRecord();
    			String fileName = this.mXmlUtilInstance.transformFullPath(table.className, blockNum);
    			list.add(fileName);
    		}else {
    			
    			int blockNumStart = table.startIndex%ConfigNozzle.getDataMaxFileRecord();
    			int blockNumEnd = table.endIndex%ConfigNozzle.getDataMaxFileRecord();
    			
    			if(blockNumStart == blockNumEnd) {
    				String fileName = this.mXmlUtilInstance.transformFullPath(table.className, blockNumStart);
    			    list.add(fileName);
    			}else {
    				for(int i = blockNumStart;i<=blockNumEnd;i++) {
    					String fileName = this.mXmlUtilInstance.transformFullPath(table.className, i);
    					list.add(fileName);
    				}
    			}
    		}
    	}
    	
    	return list;
    }
    
    
    /**
     * 1.We should combine all the changes like this:
     *   A:start 0 ,end 100 ;B:start 3 ,end 6 =>A:start 0,end 100
     *   A:start 0 ,end 100 ;B:start 101 ,end 109 =>A:start 0,end 109
     *   A:pos 100 ;pos 101  =>A:start 100,end 101
     *   
     */
     /*
     private void ChangeTableOptimizer(ArrayList<ModifyTable> changedTable) {
    	ArrayList<ModifyTable> useLessChange = new ArrayList<ModifyTable>();
    	HashMap<String,ArrayList<Integer>>map = new HashMap<String,ArrayList<Integer>>(); 
    	
    	int count = 0;
    	
    	for(ModifyTable t:changedTable) {
    		ArrayList<Integer>indexList = map.get(t.table);
    		
    		if(indexList == null) {
    			indexList = new ArrayList<Integer>();
    			indexList.add(count);
    			map.put(t.table,indexList);
    		}else {
    			Integer uselessIndex = -1;
    			
    			for(Integer index:indexList) {
    				ModifyTable modifyTable = changedTable.get(index);
    				
    				if(modifyTable.position == t.position + 1) {
    					modifyTable.startIndex = modifyTable.position;
    					modifyTable.endIndex = t.position;
    					modifyTable.position = 0;
    					useLessChange.add(t);
    					break;
    				}else if(modifyTable.startIndex <= t.startIndex && modifyTable.endIndex >= t.endIndex
    						|| modifyTable.startIndex <= t.position && modifyTable.endIndex >= t.position ) {
    					useLessChange.add(t);
    					break;
    				}else if(modifyTable.startIndex == t.endIndex + 1) {
    					t.endIndex++;
    					useLessChange.add(t);
    					break;
    				}else if(modifyTable.endIndex == t.startIndex -1) {
    					t.startIndex--;
    					useLessChange.add(t);
    					break;
    				}
    				uselessIndex = index;
    			}
    			
    			if(uselessIndex != -1) {
    			    indexList.remove(uselessIndex);
    			}
    		}
    		
    		count++;
    	}
    	 
    	changedTable.removeAll(useLessChange);
    }*/
}
