package com.trade_calc.demo.util;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MyProperties {

    private static final ILogger log = Logger.getLogger(MyProperties.class);

    public static String HAZELCAST_CONFIG;
    public static int CLUSTER_MEMBER_COUNT;

    public static boolean WAN_ENABLED;
    public static boolean WAN_PUBLISHER_ENABLED;
    public static String WAN_TARGET_URL;
    public static String WAN_TARGET_NAME;


    static Properties properties;

    public static void readAllProperties(String propFileName) {

        InputStream stream = null;
        try {
            stream = new FileInputStream(propFileName);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        if (null == stream) {
            try {
                throw new FileNotFoundException("Property file " + propFileName
                        + " not found in the classpath.  Exiting..");
            } catch (FileNotFoundException e) {
                log.severe(e);
            }
        }
        try {
            properties = new Properties();
            properties.load(stream);
            setProperties(properties);
        } catch (IOException e) {
            log.severe(e);
        }
    }

    private static void setProperties(Properties properties) {

        HAZELCAST_CONFIG = properties.getProperty("HazelcastConfiguration");
        if(HAZELCAST_CONFIG == null || HAZELCAST_CONFIG.equals("")) {
            log.severe("Hazelcast configuration not configured, please provide fully qualified path to Hazelcast configuration file in Process.properties... exiting. ");
            System.exit(-1);
        }

        String memberCount = properties.getProperty("ClusterMemberCount");
        if(memberCount == null) {
            log.warning("Hazelcast server count not configured, using default of 2");
            CLUSTER_MEMBER_COUNT = 2;
        } else
            CLUSTER_MEMBER_COUNT = Integer.parseInt(memberCount);

        String wan_enable_string = properties.getProperty("WanEnable");
        if (wan_enable_string == null) {
            log.warning("WAN Replication configuration not found. WAN replication disabled.");
            WAN_ENABLED = false;
        } else
            WAN_ENABLED = Boolean.parseBoolean(wan_enable_string);

        String wan_pub_enable_string = properties.getProperty("WanPublisherEnable");
        if (wan_pub_enable_string == null) {
            log.warning("WAN Publisher not enabled in WAN configuration, disbaling WAN Replication.");
            WAN_PUBLISHER_ENABLED = false;
        } else
            WAN_PUBLISHER_ENABLED = Boolean.parseBoolean(wan_pub_enable_string);

        WAN_TARGET_URL= properties.getProperty("WanTargetURL");
        if (WAN_ENABLED && WAN_TARGET_URL == null) {
            log.severe("WAN Target URL not found, terminating");
            System.exit(-1);
        }

        WAN_TARGET_NAME = properties.getProperty("WanTargetClusterName");
        if (WAN_ENABLED && WAN_TARGET_NAME == null) {
            log.severe("WAN Target cluster name not found, terminating");
            System.exit(-1);
        }
    }

}
