package com.localxdata.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.xml.parsers.*;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.localxdata.config.ConfigNozzle;
import com.localxdata.storage.DataCellList;
import com.localxdata.struct.DataCell;
import com.localxdata.struct.DataTableControl;
import com.localxdata.struct.ObjectMemberStruct;

public class XmlUtil {

    private DocumentBuilderFactory mFactory;
    private DocumentBuilder mBuilder;

    private static final String TAG = "XmlUtil";

    // Create File Result
    public static int RESULT_CREATE_SUCCESS = 0;
    public static int RESULT_CREATE_FAIL_FILE_EXISTS = 1;
    public static int RESULT_CREATE_FAIL_TRANFORM_CONFIG_ERROR = 2;
    public static int RESULT_CREATE_FAIL_TRANFORM_ERROR = 3;
    public static int RESULT_WRITE_SUCCESS = 4;

    public static int RESULT_PRASE_FAIL_FIELD_WITHOUT_SECURITY = 5;
    public static int RESULT_PRASE_FAIL_FIELD_NO_FIELD = 6;

    // Xml file
    public static final String FILE_EXTENSION = ".";
    public static final String XML_FILE_TAG = ".xml";

    // data Xml Element
    public static final String DATA_ELEMENT_TAG_DATA = "data";
    public static final String DATA_ELEMENT_TAG_ATTR = "attr";
    
    // data table Xml Element
    public static final String TABLE_ELEMENT_TAG_RECORD = "records";
    public static final String TABLE_ELEMENT_TAG_DATA = "data";
    public static final String TABLE_ELEMENT_TAG_TAB = "tab";
    public static final String TABLE_ELEMENT_TAG_START = "start";
    public static final String TABLE_ELEMENT_TAG_END = "end";
    

    // Xml length
    public static final int MAX_DB_SIZE = 0xFFF; // 128 Mb

    // XML Tag
    private final static String XML_TAG = "<?xml version=" + '"' + "1.0" + '"'
            + " encoding=" + '"' + "UTF-8" + '"' + " standalone=" + '"' + "no"
            + '"' + "?>";

    private static final String XML_DATA_BEGIN = "<data>";
    private static final String XML_DATA_FINAL = "</data>";

    private static final String XML_ATTR = "attr";

    private static final char[] XML_DATA_BEGIN_CHAR_ARRAY = XML_DATA_BEGIN
            .toCharArray();
    private static final int XML_DATA_START_LENGTH = 6;

    private static final char[] XML_DATA_FINAL_CHAR_ARRAY = XML_DATA_FINAL
            .toCharArray();
    private static final int XML_DATA_END_LENGTH = 7;

    private static final char[] XML_ATTR_CHAR_ARRAY = XML_ATTR.toCharArray();

    // <data>
    private static final int STATUS_FIND_TAG_IDLE = 0;

    private static final int STATUS_FIND_TAG_BEIGN_START = 1;
    private static final int STATUS_FIND_TAG_BEIGN_END = 2;

    // <member,attr = ''>
    private static final int STATUS_FIND_TAG_ATTR_START = 3;
    private static final int STATUS_FIND_TAG_ATTR_END = 4;

    // <>ffff<>
    private static final int STATUS_FIND_DATA_START = 5;
    private static final int STATUS_FIND_DATA_END = 6;

    // </data>
    private static final int STATUS_FIND_TAG_FINAL_START = 7;
    private static final int STATUS_FIND_TAG_FINAL_END = 8;

    //private HashMap<String, ArrayList<DataCell>> mDataListMap;

    private String root = "";

    private static XmlUtil mInstance;

    private XmlUtil() throws ParserConfigurationException {
        mFactory = DocumentBuilderFactory.newInstance();
        mBuilder = mFactory.newDocumentBuilder();
    }

    public static XmlUtil getInstance() {
        if (mInstance == null) {
            try {
                mInstance = new XmlUtil();
            } catch (ParserConfigurationException e) {
                e.printStackTrace();
            }
        }

        return mInstance;
    }

    public int createDataXml(String className,DataCellList datalist,int blockNum) {
        Object obj = datalist.get(0).obj;

        int result = RESULT_CREATE_SUCCESS;

        ArrayList<ObjectMemberStruct> column = PraseParamUtil
                .PraseObjectMember(obj);

        try {
            result = WriteXmlForNew(className, datalist, column,blockNum);
        } catch (IOException e) {
            return RESULT_CREATE_FAIL_TRANFORM_ERROR;
        }

        if (result != RESULT_WRITE_SUCCESS) {
            return result;
        }

        return RESULT_WRITE_SUCCESS;
    }

    public int createDataXml(String className,DataCellList datalist,DataCell startCell,DataCell endCell,int blockNum) {
    	Object obj = datalist.get(0).obj;

        int result = RESULT_CREATE_SUCCESS;

        ArrayList<ObjectMemberStruct> column = PraseParamUtil
                .PraseObjectMember(obj);

        try {
        	String fileName =  ConfigNozzle.getLocalDataEngineRoot() 
                                   + className 
                                   + "_" 
                                   + blockNum 
                                   + ".xml";

            //int startIndex = ConfigNozzle.getDataMaxFileRecord()*blockNum;
        	int startIndex = datalist.indexOf(startCell);
        	int endIndex = datalist.indexOf(endCell);
        	
            WriteXml(className,
	                 fileName,
	                 datalist,
	                 column,
	                 startIndex,
	                 endIndex);
        } catch (IOException e) {
            return RESULT_CREATE_FAIL_TRANFORM_ERROR;
        }

        if (result != RESULT_WRITE_SUCCESS) {
            return result;
        }

        return RESULT_WRITE_SUCCESS;
    }
    
    
    public int updateDataXml(String className,DataCellList datalist,DataCell startCell,DataCell endCell,int blockNum) {
    	
    	 Object obj = datalist.get(0).obj;
    	 
    	 ArrayList<ObjectMemberStruct> column = PraseParamUtil
         .PraseObjectMember(obj);
    	 
    	try {
			this.WriteXmlForUpdate(className, datalist, column, startCell, endCell,blockNum);
		} catch (IOException e) {
			e.printStackTrace();
		}
    	
    	return RESULT_WRITE_SUCCESS;
    }
    
    
    /**
     * We use btree to save xml,so the file name will be remodified like this
     * class:
     * com.sample.data =>com.sample.data_1.xml/com.sample.data_2.xml/.......
     * 
     * @return
     */
    public void LoadAllDataXml(HashMap<String, DataCellList> dataListMap,ArrayList<String>files) {
        for (String f : files) {
            String fileName = f;
            
            if (fileName.endsWith(XML_FILE_TAG)) {
                String tableName = XmlUtil.getInstance().transformClassName(fileName);//fileName.substring(0, lastIndex);
                // mDataListMap.put(tableName, LoadXml(tableName));
                // mDataListMap.put(tableName, LoadXml_Ex(tableName));

                DataCellList list = dataListMap.get(tableName);
                if(list == null) {
                    dataListMap.put(tableName, LoadDataXml_Sax(tableName,fileName));
                }else {
                	LoadDataXml_Sax(tableName,fileName);
                }
            }
        }
    }
    // we can user SAX to prase xml
    private DataCellList LoadDataXml_Sax(String className,String filename) {

        PraseXmlUtil prase = new PraseXmlUtil();
        File file = new File(filename);

        if (file.exists()) {
            InputStream stream = null;
            try {
                stream = new FileInputStream(file);
            } catch (FileNotFoundException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return null;
            }

            return prase.Prase(className, stream);
        }

        return null;
    }

    
    
    private int WriteXmlForNew(String className, DataCellList datalist,
        ArrayList<ObjectMemberStruct> column,int blockNum) throws IOException {
    	
    	
    	String fileName =  ConfigNozzle.getLocalDataEngineRoot() 
                            + className 
                            + "_" 
                            + blockNum 
                            + ".xml";
    		
    	int startIndex = ConfigNozzle.getDataMaxFileRecord()*blockNum;
    	return	WriteXml(className,
    				fileName,
    				datalist,
    				column,
    				startIndex,
    				startIndex + ConfigNozzle.getDataMaxFileRecord() - 1);
    }
    
    
    private int WriteXml(String className,String fileName, DataCellList datalist,
            ArrayList<ObjectMemberStruct> column,int start,int end) throws IOException {

        long current = System.currentTimeMillis();
        LogUtil.d(TAG, "WriteXmlForNew start at " + current);

        BufferedOutputStream out = null;

        out = new BufferedOutputStream(new FileOutputStream(fileName));

        // add TAG
        byte[] XML_TAG_BYTE = XML_TAG.getBytes();

        out.write(XML_TAG_BYTE);

        // add Start root
        String title = "<" + className + ">";
        byte[] TITLE_TAG_BYTE = title.getBytes();

        out.write(TITLE_TAG_BYTE);

        int[] datatype = new int[column.size()];
        boolean isFirstData = true;
        int countCursor = 0;

        LogUtil.d(TAG, "WriteXmlForNew start");
        
        //int startIndex = ConfigNozzle.getDataMaxFileRecord()*blockNum;
        
        int dataListSize = datalist.size();
        
        DataCellList removeList = new DataCellList();
        StringBuilder dataString = new StringBuilder();        
        datalist.startLoopRead();
        for(int i = start;i<= end;i++) {
            
            if(i >= dataListSize) {
                break;
            }
            DataCell dataCell = datalist.get(i);
            
            if(dataCell.getState() == DataCell.DATA_DELETE) {
            	removeList.add(dataCell);
            	continue;
            }
            
            countCursor = 0;
            dataString.append("<data>");
            dataString.append("<_id attr= " + '"' + "scaler" + '"' + ">");
            dataString.append(dataCell.getId());
            dataString.append("</_id>");
            
            for (ObjectMemberStruct col : column) {
                String value = null;
                String attr = null;
                try {
                    Field field = dataCell.obj.getClass().getDeclaredField(col.name);
                    field.setAccessible(true);
                    attr = col.type;

                    if (isFirstData) {
                        datatype[countCursor] = PraseParamUtil
                                .PraseObjectType(attr);
                    }

                    switch (datatype[countCursor]) {
                    case PraseParamUtil.PRASE_TYPE_INT:
                        value = String.valueOf(field.getInt(dataCell.obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_BOOLEAN:
                        value = String.valueOf(field.getBoolean(dataCell.obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_FLOAT:
                        value = String.valueOf(field.getFloat(dataCell.obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_LONG:
                        value = String.valueOf(field.getLong(dataCell.obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_STRING:
                        value = (String) field.get(dataCell.obj);
                        break;
                    }
                } catch (SecurityException e) {
                    e.printStackTrace();
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                
                dataString.append("<");
                dataString.append(col.name);
                dataString.append(" attr=");
                dataString.append('"');
                dataString.append(col.type);
                dataString.append('"');
                dataString.append(">");
                dataString.append(value);
                dataString.append("</");
                dataString.append(col.name);
                dataString.append(">");
                
                //we should remove the data finaly
                switch(dataCell.getState()) {
                    //case DataCell.DATA_DELETE:
                    	//datalist.remove(dataCell);
                    	//removeList.add(dataCell);
                    	//break;
                    	
                    case DataCell.DATA_INSERT:
                    case DataCell.DATA_UPDATE:
                    	dataCell.setState(DataCell.DATA_IDLE);
                }
                
                countCursor++;
            }

            isFirstData = false;
            dataString.append("</data>");

            byte[] dataBytes = dataString.toString().getBytes();
            out.write(dataBytes);

            out.flush();
            
            dataString.delete(0, dataString.length());
            dataString.setLength(0);
        }
        datalist.finishLoopRead();
        
        String finishTag = "</" + className + ">";
        byte[] FINISH_TAG_BYTE = finishTag.getBytes();

        out.write(FINISH_TAG_BYTE);

        out.flush();
        out.close();

        LogUtil.d(TAG, "WriteXmlForNew end at "
                + (System.currentTimeMillis() - current));

        if(removeList.size() != 0) {
        	datalist.removeAll(removeList);
        }
        
        //StringBuffer needs a lot of memory...
        //So we should call gc to collect memory
        System.gc();
        
        return RESULT_WRITE_SUCCESS;
    }
    

    //we first refresh the xml all~~
    //
    private int WriteXmlForUpdate(String className, DataCellList datalist,
            ArrayList<ObjectMemberStruct> column,DataCell startData,DataCell endData,int blockNum) throws IOException {
    	
    	String fileName =  ConfigNozzle.getLocalDataEngineRoot() 
                           + className 
                           + "_" 
                           + blockNum 
                           + ".xml";
    	
    	WriteXml(className,
				fileName,
				datalist,
				column,
				datalist.indexOf(startData),
				datalist.indexOf(endData));

    	return RESULT_WRITE_SUCCESS;
    }
    
    //
    public int CreateDataTableXml(String filename,
            HashMap<String,DataTableControl> datatableControl) 
               throws IOException {
        
        BufferedOutputStream out = null;

        out = new BufferedOutputStream(new FileOutputStream(filename));

        byte[] XML_TAG_BYTE = XML_TAG.getBytes();

        out.write(XML_TAG_BYTE);

        String title = "<" + TABLE_ELEMENT_TAG_RECORD + ">";
        byte[] TITLE_TAG_BYTE = title.getBytes();

        out.write(TITLE_TAG_BYTE);
        
        Iterator iter = datatableControl.entrySet().iterator();
        
        StringBuilder dataString = new StringBuilder();
        
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String tab = String.valueOf(entry.getKey());
            DataTableControl val = (DataTableControl)entry.getValue();

            dataString.append("<" + TABLE_ELEMENT_TAG_DATA + ">");
            dataString.append("<" + TABLE_ELEMENT_TAG_TAB + ">");
            dataString.append(tab);
            dataString.append("</" + TABLE_ELEMENT_TAG_TAB + ">");
            
            dataString.append("<" + TABLE_ELEMENT_TAG_START + ">");
            dataString.append(val.start);
            dataString.append("</" + TABLE_ELEMENT_TAG_START + ">");
            
            dataString.append("<" + TABLE_ELEMENT_TAG_END + ">");
            dataString.append(val.end);
            dataString.append("</" + TABLE_ELEMENT_TAG_END + ">");
            
            dataString.append("</" + TABLE_ELEMENT_TAG_DATA + ">");

            byte[] dataBytes = dataString.toString().getBytes();
            out.write(dataBytes);
            out.flush();
            
            dataString.delete(0, dataString.length());
            dataString.setLength(0);
        }

        String finishTag = "</" + TABLE_ELEMENT_TAG_RECORD + ">";
        byte[] FINISH_TAG_BYTE = finishTag.getBytes();

        out.write(FINISH_TAG_BYTE);

        out.flush();
        out.close();

        return RESULT_WRITE_SUCCESS;
    }
    
    public void LoadDataTableXml(String filename,HashMap<String,DataTableControl> tableMap) {
        
        try {
            Document doc = mBuilder.parse(filename);

            Element root = doc.getDocumentElement();

            NodeList elementList = root.getChildNodes();

            /**
             * the data class can not create constructor
             * 
             * */

            for (int j = 0; j < elementList.getLength(); j++) {
                
                DataTableControl data = new DataTableControl(0,0);

                Element node = (Element) elementList.item(j);

                if (!node.getNodeName().equals(TABLE_ELEMENT_TAG_DATA)) {
                    continue;
                }

                NodeList dataNodeList = node.getChildNodes();

                String fileName = "";
                
                for (int k = 0; k < dataNodeList.getLength(); k++) {
                    Element dataNode = (Element) dataNodeList.item(k);
                    String txt = dataNode.getTextContent();
                        
                    if(dataNode.getNodeName().equals(TABLE_ELEMENT_TAG_TAB)) {
                        fileName = txt;
                    } else if(dataNode.getNodeName().equals(TABLE_ELEMENT_TAG_START)) {
                        data.start = Integer.valueOf(txt);
                    } else if(dataNode.getNodeName().equals(TABLE_ELEMENT_TAG_END)) {
                        data.end = Integer.valueOf(txt);
                    }
                }
                tableMap.put(fileName+".xml", data);
            }
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }       
    }
    
    public String transformFileName(String classname,int blocknum) {
    	String fileName = classname + "_" + blocknum + ".xml";
    	return fileName;
    }
    
    public int transformBlockNum(String filename) {
    	
    	//because String.substring may make the memory be received difficaultly.
    	//so we user construct to remove string's reference
    	String name = new String(filename.substring(filename.lastIndexOf("_") + 1, 
				filename.lastIndexOf(".xml")));
    	
    	int blockNum = Integer.valueOf(name);
    	
    	return blockNum;
    }
    
    public String transformClassName(String filename) {
    	return filename.substring(0,filename.lastIndexOf("_"));
    }
    
    public String transformFullPath(String classname,int blockNum) {
    	return  ConfigNozzle.getLocalDataEngineRoot() 
                    + classname
                    + "_" 
                    + blockNum 
                    + ".xml";
    }
}
