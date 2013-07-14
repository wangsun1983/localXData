package com.localxdata.util;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.RandomAccessFile;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.*;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import java.nio.BufferOverflowException;

import com.localxdata.struct.ObjectMemberStruct;

public class XmlUtil {

    private DocumentBuilderFactory mFactory;
    private DocumentBuilder mBuilder;
    private Document mDoc;

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

    // Xml Element
    public static final String ELEMENT_TAG_DATA = "data";
    public static final String ELEMENT_TAG_ATTR = "attr";

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

    private HashMap<String, ArrayList<Object>> mDataListMap;

    private String root = "";

    private static XmlUtil mInstance;

    private XmlUtil() throws ParserConfigurationException {
        mFactory = DocumentBuilderFactory.newInstance();
        mBuilder = mFactory.newDocumentBuilder();

        mDoc = mBuilder.newDocument();
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

    public void setXmlRootDir(String path) {
        root = path;
    }

    public int CreateXml(String filename) {
        File file = new File(root + filename + XML_FILE_TAG);

        if (!file.exists() || !file.isFile()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                return RESULT_CREATE_FAIL_FILE_EXISTS;
            }
        }

        StreamResult streamResult = new StreamResult(file);
        Source inputSource = new DOMSource(mDoc);
        TransformerFactory transformerFactory = TransformerFactory
                .newInstance();
        Transformer transformer;

        try {
            transformer = transformerFactory.newTransformer();
            transformer.transform(inputSource, streamResult);
        } catch (TransformerConfigurationException e) {
            return RESULT_CREATE_FAIL_TRANFORM_CONFIG_ERROR;
        } catch (TransformerException e) {
            return RESULT_CREATE_FAIL_TRANFORM_ERROR;
        }

        return RESULT_CREATE_SUCCESS;
    }

    @SuppressWarnings("unchecked")
    public int CreateXml(ArrayList data) {
        Object obj = data.get(0);

        String tableName = PraseParamUtil.PraseObjectName(obj);

        int result = RESULT_CREATE_SUCCESS;

        // wangsl
        // ArrayList<String>column = PraseParamUtil.PraseObjectMember(obj);
        ArrayList<ObjectMemberStruct> column = PraseParamUtil
                .PraseObjectMember(obj);
        // wagnsl

        try {
            result = WriteXmlForNew(tableName, data, column);
        } catch (IOException e) {
            return RESULT_CREATE_FAIL_TRANFORM_ERROR;
        }

        if (result != RESULT_WRITE_SUCCESS) {
            return result;
        }

        return RESULT_WRITE_SUCCESS;
    }

    public HashMap<String, ArrayList<Object>> LoadAllXml() {
        if (mDataListMap != null) {
            return mDataListMap;
        }

        File directory = new File(root);
        File file = new File(directory.getAbsolutePath());
        mDataListMap = new HashMap<String, ArrayList<Object>>();

        File[] files = file.listFiles();

        for (File f : files) {
            if (f.isFile()) {
                String fileName = f.getName();

                if (fileName.endsWith(XML_FILE_TAG)) {
                    String tableName = fileName.substring(0, fileName
                            .lastIndexOf(FILE_EXTENSION));
                    // mDataListMap.put(tableName, LoadXml(tableName));
                    mDataListMap.put(tableName, LoadXml_Sax(tableName));

                }
            }
        }
        return mDataListMap;
    }

    public ArrayList<Object> LoadXml(String className) {

        long current = System.currentTimeMillis();
        LogUtil.d(TAG, "LoadXml start at " + current);

        ArrayList<Object> list = mDataListMap.get(className);
        if (list != null) {
            return list;
        }

        list = new ArrayList<Object>();

        try {
            Document doc = mBuilder.parse(root + className + XML_FILE_TAG);

            Element root = doc.getDocumentElement();

            String clname = root.getNodeName();

            NodeList elementList = root.getChildNodes();

            LogUtil.d(TAG, "LoadXml start trace "
                    + (System.currentTimeMillis() - current));

            Class clazz = Class.forName(clname);
            Constructor[] constructorList = clazz.getDeclaredConstructors();

            int constructorLength = constructorList.length;

            if (constructorLength > 1) {
                return null;
            }

            /**
             * the data class can not create constructor
             * 
             * */
            Constructor constructor = constructorList[0];
            if (constructor.getParameterTypes().length > 0) {
                return null;
            }
            constructor.setAccessible(true);

            // We should create index.......
            boolean isFirst = true;
            // We should create index.......

            for (int j = 0; j < elementList.getLength(); j++) {
                Object membet = constructor.newInstance();
                Field[] fieldlist = membet.getClass().getDeclaredFields();

                Element node = (Element) elementList.item(j);

                if (!node.getNodeName().equals(ELEMENT_TAG_DATA)) {
                    continue;
                }

                NodeList dataNodeList = node.getChildNodes();

                for (int k = 0; k < dataNodeList.getLength(); k++) {
                    Element dataNode = (Element) dataNodeList.item(k);
                    for (Field field : fieldlist) {
                        if (dataNode.getNodeName().equals(field.getName())) {
                            field.setAccessible(true);
                            // wangsl
                            // We should create index.......
                            // TODO
                            // wangsl
                            switch (PraseParamUtil.PraseObjectType(dataNode
                                    .getAttribute(ELEMENT_TAG_ATTR))) {
                            case PraseParamUtil.PRASE_TYPE_INT:
                                int intValue = Integer.valueOf(dataNode
                                        .getTextContent());
                                field.setInt(membet, intValue);
                                break;

                            case PraseParamUtil.PRASE_TYPE_BOOLEAN:
                                Boolean boolValue = Boolean.valueOf(dataNode
                                        .getTextContent());
                                field.setBoolean(membet, boolValue);
                                break;

                            case PraseParamUtil.PRASE_TYPE_FLOAT:
                                float floatValue = Float.valueOf(dataNode
                                        .getTextContent());
                                field.setFloat(membet, floatValue);
                                break;

                            case PraseParamUtil.PRASE_TYPE_LONG:
                                Long longValue = Long.valueOf(dataNode
                                        .getTextContent());
                                field.setLong(membet, longValue);
                                break;

                            case PraseParamUtil.PRASE_TYPE_STRING:
                                String stringValue = String.valueOf(dataNode
                                        .getTextContent());
                                field.set(membet, stringValue);
                                break;
                            }
                        }
                    }
                }
                list.add(membet);
            }
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }

        LogUtil.d(TAG, "LoadXml end at "
                + (System.currentTimeMillis() - current));
        return list;
    }

    // this function is faster....
    public ArrayList<Object> LoadXml_Ex(String className) {
        long current = System.currentTimeMillis();
        LogUtil.d(TAG, "LoadXml_Ex start at " + current);

        ArrayList<Object> list = mDataListMap.get(className);
        if (list != null) {
            return list;
        }

        Class clazz;
        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e1) {
            return null;
        }

        Constructor[] constructorList = clazz.getDeclaredConstructors();

        int constructorLength = constructorList.length;

        if (constructorLength > 1) {
            return null;
        }

        /**
         * the data class can not create constructor
         * 
         * */
        Constructor constructor = constructorList[0];
        if (constructor.getParameterTypes().length > 0) {
            return null;
        }
        constructor.setAccessible(true);

        // Use Index to fasten search,so we must to creat index first start
        Field[] field_list = clazz.getDeclaredFields();

        File file = new File(root + className + XML_FILE_TAG);

        FileReader fileReader;

        list = new ArrayList<Object>();
        try {
            fileReader = new FileReader(file);
            BufferedReader reader = new BufferedReader(fileReader);

            long end = file.length();
            System.out.println("end is " + end);

            int READ_LENGTH = 100 * 1024;

            char buffer[] = new char[READ_LENGTH];
            boolean isFinish = false;

            int findDataNum = 0;

            int[] typeList = null;
            int fieldLength = field_list.length;
            int anylizeMemberIndex = 0;
            int anylizeTypeIndex = 0;
            int anylizeIndex = 0;
            int currentStatus = STATUS_FIND_TAG_IDLE;
            int attrStartIndex = 0;
            int attrEndIndex = 0;
            int readSize = 0;
            System.out.println("start at " + System.currentTimeMillis());
            Object membet = null;
            Field[] fieldlist = null;

            while (!isFinish) {

                if (anylizeIndex != 0 && anylizeIndex < buffer.length) {

                    char[] tempBuffer = new char[READ_LENGTH];
                    readSize = reader.read(tempBuffer);

                    char[] finalBuffer = new char[buffer.length - anylizeIndex
                            + READ_LENGTH];

                    System.arraycopy(buffer, anylizeIndex, finalBuffer, 0,
                            buffer.length - anylizeIndex);
                    System.arraycopy(tempBuffer, 0, finalBuffer, buffer.length
                            - anylizeIndex - 1, READ_LENGTH);

                    buffer = finalBuffer;

                    if (currentStatus != STATUS_FIND_TAG_ATTR_START) {
                        attrStartIndex = 0;
                        attrEndIndex = 0;
                        anylizeIndex = 0;
                    } else {
                        if (attrStartIndex != 0) {
                            attrStartIndex = attrStartIndex - anylizeIndex;
                            anylizeIndex = attrStartIndex + 1;
                        } else {
                            anylizeIndex = 0;
                        }

                    }
                } else {
                    readSize = reader.read(buffer);
                    anylizeIndex = 0;
                    attrStartIndex = 0;
                    attrEndIndex = 0;
                }

                if (readSize == -1) {
                    if (anylizeIndex == buffer.length) {
                        break;
                    }

                    isFinish = true;
                }
                // if it is first anylize,we should check its type...

                boolean isAnylizeFinish = false;
                while (!isAnylizeFinish) {

                    if (anylizeMemberIndex == 0) {
                        membet = constructor.newInstance();
                        fieldlist = membet.getClass().getDeclaredFields();
                        if (typeList == null) {
                            typeList = new int[fieldlist.length];
                        }
                    }

                    if (anylizeIndex >= buffer.length - 1) {
                        break;
                    }

                    if (buffer[anylizeIndex] == ' ') {
                        anylizeIndex++;
                        continue;
                    }

                    int restAnylize = buffer.length - anylizeIndex;
                    // System.out.println("restAnylize = " + restAnylize);
                    // System.out.println("current status is " + currentStatus);
                    switch (currentStatus) {
                    case STATUS_FIND_TAG_IDLE:
                        if (buffer[anylizeIndex] == XML_DATA_BEGIN_CHAR_ARRAY[0]) {
                            if (anylizeIndex + XML_DATA_START_LENGTH > buffer.length) {
                                break;
                            }

                            if (buffer[anylizeIndex + XML_DATA_START_LENGTH - 1] == XML_DATA_BEGIN_CHAR_ARRAY[XML_DATA_START_LENGTH - 1]) {
                                currentStatus = STATUS_FIND_TAG_BEIGN_START;
                                anylizeIndex = anylizeIndex
                                        + XML_DATA_START_LENGTH;
                                findDataNum++;
                            }
                        }
                        anylizeIndex++;
                        break;

                    case STATUS_FIND_TAG_BEIGN_START:
                        if (anylizeIndex + XML_DATA_START_LENGTH > buffer.length) {
                            break;
                        }

                        anylizeIndex = anylizeIndex + XML_DATA_START_LENGTH;
                        currentStatus = STATUS_FIND_TAG_ATTR_START;
                        break;

                    case STATUS_FIND_TAG_ATTR_START:
                        // if this is the first time to anylize,we should
                        // anylize attr
                        if (findDataNum == 1) {
                            int findAttrIndex = restAnylize;
                            int backAnylizeIndex = anylizeIndex;

                            for (; findAttrIndex > 0; findAttrIndex--, backAnylizeIndex++) {
                                if (buffer[backAnylizeIndex] == '"'
                                        && attrStartIndex == 0) {
                                    attrStartIndex = backAnylizeIndex;
                                    attrEndIndex = 0;
                                } else if (buffer[backAnylizeIndex] == '"'
                                        && attrEndIndex == 0) {
                                    attrEndIndex = backAnylizeIndex;
                                    String type = String.valueOf(buffer,
                                            attrStartIndex + 1, attrEndIndex
                                                    - attrStartIndex - 1);
                                    attrStartIndex = 0;
                                    currentStatus = STATUS_FIND_DATA_START;
                                    if (anylizeTypeIndex < fieldLength) {
                                        typeList[anylizeTypeIndex] = PraseParamUtil
                                                .PraseObjectType(type);
                                        anylizeTypeIndex++;
                                    }
                                    break;
                                }
                            }
                            if (findAttrIndex == 0) {
                                isAnylizeFinish = true;
                            } else {
                                anylizeIndex = backAnylizeIndex + 1;
                            }
                        } else {
                            int findAttrIndex = restAnylize;
                            int backAnylizeIndex = anylizeIndex;
                            boolean maybeAttr = false;
                            for (; findAttrIndex > 0; findAttrIndex--, backAnylizeIndex++) {
                                if (buffer[backAnylizeIndex] == '"') {
                                    maybeAttr = true;
                                } else if (maybeAttr
                                        && buffer[backAnylizeIndex] == '>') {
                                    break;
                                }
                            }

                            if (findAttrIndex == 0) {
                                isAnylizeFinish = true;
                            } else {
                                anylizeIndex = backAnylizeIndex;
                                currentStatus = STATUS_FIND_DATA_START;
                            }
                        }
                        break;

                    case STATUS_FIND_DATA_START:
                        int findDataIndex = restAnylize;
                        int backAnylizeIndex = anylizeIndex;
                        boolean mayBeData = false;
                        for (; findDataIndex > 0; findDataIndex--, backAnylizeIndex++) {
                            if (buffer[backAnylizeIndex] == '<') {
                                // TODO we should change data
                                mayBeData = true;

                            } else if (mayBeData
                                    && buffer[backAnylizeIndex] == '/') {
                                // TODO </ can not be used in data......
                                // ohhhh,my god~~~
                                String data = String.valueOf(buffer,
                                        anylizeIndex + 1, backAnylizeIndex
                                                - anylizeIndex - 2);// because
                                                                    // of < & /
                                // System.out.println("data is " + data);
                                Field field = fieldlist[anylizeMemberIndex];
                                field.setAccessible(true);
                                switch (typeList[anylizeMemberIndex]) {
                                case PraseParamUtil.PRASE_TYPE_INT:
                                    int intValue = Integer.valueOf(data);
                                    field.setInt(membet, intValue);
                                    break;

                                case PraseParamUtil.PRASE_TYPE_BOOLEAN:
                                    Boolean boolValue = Boolean.valueOf(data);
                                    field.setBoolean(membet, boolValue);
                                    break;

                                case PraseParamUtil.PRASE_TYPE_FLOAT:
                                    float floatValue = Float.valueOf(data);
                                    field.setFloat(membet, floatValue);
                                    break;

                                case PraseParamUtil.PRASE_TYPE_LONG:
                                    Long longValue = Long.valueOf(data);
                                    field.setLong(membet, longValue);
                                    break;

                                case PraseParamUtil.PRASE_TYPE_STRING:
                                    String stringValue = String.valueOf(data);
                                    field.set(membet, stringValue);
                                    break;
                                }

                                anylizeMemberIndex++;

                                if (anylizeMemberIndex == fieldlist.length) {
                                    findDataNum++;
                                    list.add(membet);
                                    anylizeMemberIndex = 0;
                                    membet = constructor.newInstance();
                                    fieldlist = membet.getClass()
                                            .getDeclaredFields();
                                }

                                currentStatus = STATUS_FIND_TAG_ATTR_START;
                                break;
                            }
                        }

                        if (findDataIndex == 0) {
                            isAnylizeFinish = true;
                        } else {
                            anylizeIndex = backAnylizeIndex + 1;
                        }
                        break;

                    }
                }
            }
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        LogUtil.d(TAG, "LoadXml_Ex end at " + System.currentTimeMillis());

        return list;
    }

    // wangsl

    // we can user SAX to prase xml
    public ArrayList<Object> LoadXml_Sax(String className) {

        ArrayList<Object> list = mDataListMap.get(className);
        if (list != null) {
            return list;
        }

        PraseXmlUtil prase = new PraseXmlUtil();
        File file = new File(root + className + XML_FILE_TAG);

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

    private int WriteXmlForNew(String tableName, ArrayList<Object> data,
            ArrayList<ObjectMemberStruct> column) throws IOException {

        long current = System.currentTimeMillis();
        LogUtil.d(TAG, "WriteXmlForNew start at " + current);

        BufferedOutputStream out = null;

        out = new BufferedOutputStream(new FileOutputStream(tableName + ".xml"));

        // add TAG
        byte[] XML_TAG_BYTE = XML_TAG.getBytes();

        out.write(XML_TAG_BYTE);

        // add Start root
        String title = "<" + tableName + ">";
        byte[] TITLE_TAG_BYTE = title.getBytes();

        out.write(TITLE_TAG_BYTE);

        int[] datatype = new int[column.size()];
        boolean isFirstData = true;
        int countCursor = 0;

        LogUtil.d(TAG, "WriteXmlForNew start");

        for (Object obj : data) {
            countCursor = 0;
            StringBuffer dataString = new StringBuffer("<data>");

            for (ObjectMemberStruct col : column) {
                String value = null;
                String attr = null;
                try {
                    Field field = obj.getClass().getDeclaredField(col.name);
                    field.setAccessible(true);
                    attr = col.type;

                    if (isFirstData) {
                        datatype[countCursor] = PraseParamUtil
                                .PraseObjectType(attr);
                    }

                    switch (datatype[countCursor]) {
                    case PraseParamUtil.PRASE_TYPE_INT:
                        value = String.valueOf(field.getInt(obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_BOOLEAN:
                        value = String.valueOf(field.getBoolean(obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_FLOAT:
                        value = String.valueOf(field.getFloat(obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_LONG:
                        value = String.valueOf(field.getLong(obj));
                        break;

                    case PraseParamUtil.PRASE_TYPE_STRING:
                        value = (String) field.get(obj);
                        break;
                    }
                } catch (SecurityException e) {
                    e.printStackTrace();
                } catch (NoSuchFieldException e) {
                    e.printStackTrace();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                // dataString = dataString + "<" + col.name + " attr=" + '"' +
                // col.type + '"' + ">"
                // + String.valueOf(value) + "</" + col.name + ">";

                // dataString.append("<" + col.name + " attr=" + '"' + col.type
                // + '"' + ">"
                // + String.valueOf(value) + "</" + col.name + ">");

                dataString.append("<");
                dataString.append(col.name);
                dataString.append(" attr=");
                dataString.append('"');
                dataString.append(col.type);
                dataString.append('"');
                dataString.append(">");
                dataString.append(String.valueOf(value));
                dataString.append(String.valueOf("</"));
                dataString.append(String.valueOf(col.name));
                dataString.append(String.valueOf(">"));

                countCursor++;
            }

            isFirstData = false;
            // dataString += "</data>";
            dataString.append("</data>");

            byte[] dataBytes = dataString.toString().getBytes();
            out.write(dataBytes);
            out.flush();
        }

        String finishTag = "</" + tableName + ">";
        byte[] FINISH_TAG_BYTE = finishTag.getBytes();

        out.write(FINISH_TAG_BYTE);

        out.flush();
        out.close();

        LogUtil.d(TAG, "WriteXmlForNew end at "
                + (System.currentTimeMillis() - current));

        return RESULT_WRITE_SUCCESS;
    }
}
