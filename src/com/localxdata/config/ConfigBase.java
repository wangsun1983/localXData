/** 
 * 
 * This Class defines some items which can be set
 * by user.
 * 
 * @author Sunli.Wang
 *
 */

package com.localxdata.config;

public interface ConfigBase {
	//ID
	//DataBase 
    public static final String CONFIG_DATA_FILE_RECORD_NUM = "config.database.recordnum";
    public static final String CONFIG_WORK_THREAD_POOL_SIZE = "config.database.poolnum";
    
    
    //Default Value
    public static final int NOT_SET = -1;
    
    public static final int DATA_FILE_RECORD_NUM = 5000;

    public static final int WORK_THREAD_POOL_SIZE = NOT_SET;
}
