package com.localxdata.util;

import java.lang.reflect.Field;
import java.util.ArrayList;

import com.localxdata.struct.ObjectMemberStruct;


public class PraseParamUtil {
    
    public static final int PRASE_TYPE_INT = 0;
    public static final int PRASE_TYPE_LONG = 1;
    public static final int PRASE_TYPE_STRING = 2;
    public static final int PRASE_TYPE_BOOLEAN = 3;
    public static final int PRASE_TYPE_FLOAT = 4;    
    public static final int PRASE_TYPE_MAX = 5;   
    public static final int PRASE_TYPE_ERROR = 100;

    
    
    
    public static String PraseObjectName(Object obj) {
        return obj.getClass().getName();
        
    }
    
    /**
     * 
     * 
     * 1.we should return the member's name & the member's type
     *   so the method which calls this API need not to check data type
     *   again.
     */
    public static ArrayList<ObjectMemberStruct> PraseObjectMember(Object obj) {
        
        Field field[] = obj.getClass().getDeclaredFields();
        ArrayList<ObjectMemberStruct> list = new ArrayList<ObjectMemberStruct>();
        
        for(Field f:field) {
            //list.add(f.getName());
        	ObjectMemberStruct member = new ObjectMemberStruct();
        	member.name = f.getName();
        	member.type = f.getType().getName();
        	list.add(member);
        }
        
        return list;
    }
    
    public static int PraseObjectType(String type) {
        
        if(type.equals(int.class.getName())) {
            return PRASE_TYPE_INT;
        }
        
        if(type.equals(Long.class.getName())) {
            return PRASE_TYPE_LONG;
        }
         
        if(type.equals(String.class.getName())) {
            return PRASE_TYPE_STRING;
        }
        
        if(type.equals(Boolean.class.getName())) {
            return PRASE_TYPE_BOOLEAN;
        }
        
        if(type.equals(Float.class.getName())) {
            return PRASE_TYPE_FLOAT;
        }
        
        return PRASE_TYPE_ERROR ;
    }
}
