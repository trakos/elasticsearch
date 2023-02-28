/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;

import java.io.IOException;
import java.net.HttpURLConnection;

/**
 * This class is adds basic authentication to the connection,
 * and it is used by custom filesystem for knox swebhdfs.
 */
public class BasicAuthConfigurator implements ConnectionConfigurator {
    ConnectionConfigurator parent;
    String basicAuth;

    public BasicAuthConfigurator(ConnectionConfigurator parent, String basicAuth) {
        this.parent = parent;
        this.basicAuth = basicAuth;
    }

    @Override
    public HttpURLConnection configure(HttpURLConnection conn) throws IOException {
        if (this.parent != null) {
            parent.configure(conn);
        }

        if (basicAuth != null && basicAuth.equals("") == false) {
            conn.setRequestProperty("Authorization", basicAuth);
        }

        return conn;
    }
}
