package com.localxdata.config;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigNozzle implements ConfigBase{
    public static final String CONFIG_LOCAL_DATA_ENGINE_ROOT = "config.root";
    
    private static Properties props = new  Properties();;
    
    private static String mLocalDataEngineRoot = "";
    
    public static void setLocalDataEngineRoot(String root) {
        mLocalDataEngineRoot = root;
        
        try {
			props.load(new FileInputStream("config.properties"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public static String getLocalDataEngineRoot() {
        return mLocalDataEngineRoot;
    }
    
    //Config about DataBase
    public static int getDataMaxFileRecord() {
    	String value = (String) props.get(CONFIG_DATA_FILE_RECORD_NUM);
    	if(value != null) {
    	    return Integer.valueOf(value);
    	}
    	
    	return DATA_FILE_RECORD_NUM;
    }    
    
}
