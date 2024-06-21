package com.jimmy.groot.platform.constant;

public interface ConfigConstant {

    String SERIALIZE_TYPE = "serialize_type";

    interface Client {

        String SERVER_ADDRESS = "server_address";

        String ENGINE_ID = "engine_id";

        String IGNORED_NETWORK_INTERFACES = "ignored_network_interfaces";

        String PREFERRED_NETWORKS = "preferred_networks";
    }

    interface Center {

        String BOOT_PORT = "boot_port";
    }
}
