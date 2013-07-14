package com.localxdata.util;

public class LogUtil {
    
    private static final boolean DEBUG = true;
    
    public static void d(String tag,String str) {
        if(DEBUG) {
            System.out.println("Debug: [" + tag + "]" + "," + str);
        }
    } 
    
    public static void e(String tag,String str) {
        System.out.println("Error: [" + tag + "]" + "," + str);        
    }
}
