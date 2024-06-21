package com.jimmy.groot.engine.core;

import cn.hutool.core.util.IdUtil;
import cn.hutool.core.util.StrUtil;
import com.jimmy.groot.engine.exception.EngineException;
import com.jimmy.groot.engine.utils.DockerUtil;
import com.jimmy.groot.platform.constant.ConfigConstant;
import lombok.extern.slf4j.Slf4j;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.UnknownHostException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

@Slf4j
public class ConfigLoad {

    private static final String ENGINE_PROPERTIES_PATH = "engine.properties";

    private Properties properties;

    private InetAddress ipAddress;

    private static ConfigLoad configLoad;

    private ConfigLoad() {
        loadProperties();
    }

    public static String getLocalIpAddress() {
        try {
            if (configLoad.ipAddress != null) {
                return configLoad.ipAddress.getHostAddress();
            }

            throw new EngineException("获取ip地址失败");
        } catch (EngineException e) {
            throw e;
        } catch (Exception e) {
            log.error("获取ip地址失败", e);
            throw new EngineException("获取ip地址失败");
        }
    }

    public static String get(String key) {
        if (configLoad == null) {
            synchronized (ConfigLoad.class) {
                if (configLoad == null) {
                    configLoad = new ConfigLoad();
                    configLoad.ipAddress = findFirstNonLoopBackAddress();
                }
            }
        }
        return configLoad.properties.getProperty(key);
    }

    public static String getId() {
        String s = get(ConfigConstant.Client.ENGINE_ID);

        if (StrUtil.isEmpty(s)) {
            s = IdUtil.simpleUUID();

            try (OutputStream output = Files.newOutputStream(Paths.get(ENGINE_PROPERTIES_PATH))) {
                configLoad.properties.setProperty(ConfigConstant.Client.ENGINE_ID, s);
                // 将属性写入到属性文件
                configLoad.properties.store(output, null);
            } catch (IOException e) {
                throw new EngineException("properties 写入失败");
            }
        }

        String taskSlot = DockerUtil.getTaskSlot();
        if (StrUtil.isNotEmpty(taskSlot)) {
            s = s + "-" + taskSlot;
        }

        return s;
    }

    /**
     * 读取配置
     *
     * @return
     */
    private void loadProperties() {
        try (FileInputStream fileInputStream = new FileInputStream(ENGINE_PROPERTIES_PATH); InputStreamReader inputStreamReader = new InputStreamReader(fileInputStream); BufferedReader bfReader = new BufferedReader(inputStreamReader)) {
            this.properties = new Properties();
            properties.load(bfReader);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取ip地址
     *
     * @return
     */
    private static InetAddress findFirstNonLoopBackAddress() {
        InetAddress result = null;
        try {
            int lowest = Integer.MAX_VALUE;
            for (Enumeration<NetworkInterface> nics = NetworkInterface.getNetworkInterfaces(); nics.hasMoreElements(); ) {
                NetworkInterface ifc = nics.nextElement();
                if (ifc.isUp()) {
                    if (ifc.getIndex() < lowest || result == null) {
                        lowest = ifc.getIndex();
                    } else {
                        continue;
                    }

                    if (!ignoreNetWorkInterface(ifc.getDisplayName())) {
                        for (Enumeration<InetAddress> addrs = ifc.getInetAddresses(); addrs.hasMoreElements(); ) {
                            InetAddress address = addrs.nextElement();
                            if (address instanceof Inet4Address && !address.isLoopbackAddress() && isPreferredAddress(address)) {
                                result = address;
                            }
                        }
                    }
                }
            }
        } catch (IOException ex) {
            log.error("Cannot get first non-loopback address", ex);
        }

        if (result != null) {
            return result;
        }

        try {
            return InetAddress.getLocalHost();
        } catch (UnknownHostException e) {
            log.warn("Unable to retrieve localhost");
        }

        return null;
    }


    /**
     * 首选ip地址判断
     *
     * @param address
     * @return
     */
    private static boolean isPreferredAddress(InetAddress address) {
        String s = get(ConfigConstant.Client.PREFERRED_NETWORKS);
        if (StrUtil.isEmpty(s)) {
            return true;
        }

        List<String> preferredNetworks = StrUtil.split(s, ",");
        for (String regex : preferredNetworks) {
            final String hostAddress = address.getHostAddress();
            if (hostAddress.matches(regex) || hostAddress.startsWith(regex)) {
                return true;
            }
        }

        return false;
    }

    /**
     * 网络接口过滤
     *
     * @param interfaceName
     * @return
     */
    private static boolean ignoreNetWorkInterface(String interfaceName) {
        String s = get(ConfigConstant.Client.IGNORED_NETWORK_INTERFACES);
        if (StrUtil.isNotEmpty(s)) {
            List<String> split = StrUtil.split(s, ",");
            for (String regex : split) {
                if (interfaceName.matches(regex)) {
                    return true;
                }
            }
        }

        return false;
    }
}
