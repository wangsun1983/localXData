package com.localxdata.util;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.localxdata.index.IndexUtil;

import sun.rmi.runtime.Log;

public class PraseXmlUtil {

    public static final String TAG = "PraseXmlUtil";
    private int mFieldLength = 0;
    private int mFieldIndex = 0;
    private String className = null;
    private ArrayList<Object> mResultList = new ArrayList<Object>();
    private String mNodeName;
    private String mNodeValue;
    //private HashMap<String,Integer>mTypeHashMap = new HashMap<String,Integer>();
    private ArrayList<Integer>mTypeHashMap = new ArrayList<Integer>();
    private int memberIndex;
    private Object mMember;
    private boolean isFristData = true;
    private Constructor mConstructor;
    
    private Field []mFieldList;
    //
    private String mTableName;
    

    class SaxParseXml extends DefaultHandler {

        @Override
        public void startDocument() throws SAXException {
            // TODO nothing
        }

        @Override
        public void startElement(String uri, String localName, String qName,
                Attributes attributes) throws SAXException {
            if(!isFristData) {
                return;    
            }
            
            if (attributes.getLength() == 0) {
                mNodeName = null;
                return;
            }
            
            mNodeName = qName;
            //mTypeHashMap.put(mNodeName,PraseParamUtil.PraseObjectType(attributes.getValue(0)));
            mTypeHashMap.add(PraseParamUtil.PraseObjectType(attributes.getValue(0)));
        }

        @Override
        public void endElement(String uri, String localName, String qName)
                throws SAXException {
            //TODO nothing
        }

        @Override
        public void endDocument() throws SAXException {
            //TODO nothing
        }
        
        @Override
        public void characters(char[] ch, int start, int length)
                throws SAXException {
            if(mNodeName!=null){
                try {
                    String dataNode=new String(ch,start,length);
                    Field field = mFieldList[memberIndex];
                    switch(mTypeHashMap.get(memberIndex)) {
                        case PraseParamUtil.PRASE_TYPE_INT:
                            int intValue = Integer.valueOf(dataNode);
                            field.setInt(mMember, intValue);
                            break;

                        case PraseParamUtil.PRASE_TYPE_BOOLEAN:
                            Boolean boolValue = Boolean.valueOf(dataNode);
                            field.setBoolean(mMember, boolValue);
                            break;

                        case PraseParamUtil.PRASE_TYPE_FLOAT:
                            float floatValue = Float.valueOf(dataNode);
                            field.setFloat(mMember, floatValue);
                            break;

                        case PraseParamUtil.PRASE_TYPE_LONG:
                            Long longValue = Long.valueOf(dataNode);
                            field.setLong(mMember, longValue);
                            break;

                        case PraseParamUtil.PRASE_TYPE_STRING:
                            String stringValue = String.valueOf(dataNode);
                            field.set(mMember, stringValue);
                            break;
                    }
                
                    memberIndex++;
                    if(memberIndex == mFieldList.length) {
                        mResultList.add(mMember);
                        //wangsl use index to fasten search
                        
                        //LogUtil.d(TAG, "add index start at " + System.currentTimeMillis());
                        IndexUtil.getInstance().insertIndex(mMember);
                        //LogUtil.d(TAG, "add index end at " + System.currentTimeMillis());
                        
                        //wangsl use index to fasten search
                        isFristData = false;
                        memberIndex = 0;
                        mMember = mConstructor.newInstance();
                        mFieldList = mMember.getClass().getDeclaredFields();
                   }
               }catch (Exception e) {
            	   e.printStackTrace();
               }
            }
        }
    }

    public ArrayList<Object> Prase(String className,InputStream stream) {
        SAXParser parser = null;
        try {
            parser = SAXParserFactory.newInstance().newSAXParser();
            SaxParseXml parseXml = new SaxParseXml();
                
            Class clazz = Class.forName(className);
            Constructor[] constructorList = clazz.getDeclaredConstructors();

            int constructorLength = constructorList.length;
            
            if (constructorLength > 1) {
                return null;
            }

            /**
             * the data class can not create constructor
             * 
             * */
            mConstructor = constructorList[0];
            if (mConstructor.getParameterTypes().length > 0) {
                return null;
            }
            mConstructor.setAccessible(true);
            
            //we initate first
            mMember = mConstructor.newInstance();
            mFieldList = mMember.getClass().getDeclaredFields();
            
            parser.parse(stream, parseXml);
            
        } catch (Exception e) {
            System.out.println(e);
        }
        
        return mResultList;
    }
}
