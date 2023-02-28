/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.hadoop.hdfs.web;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.client.HdfsClientConfigKeys;
import org.apache.hadoop.hdfs.web.oauth2.OAuth2ConnectionConfigurator;
import org.apache.hadoop.hdfs.web.resources.HttpOpParam;
import org.apache.hadoop.hdfs.web.resources.Param;
import org.apache.hadoop.security.authentication.client.ConnectionConfigurator;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.GeneralSecurityException;
import java.util.concurrent.TimeUnit;

/**
 * This class is used by custom filesystem for knox swebhdfs.
 */
public class KnoxSWebHdfsFileSystem extends SWebHdfsFileSystem {
    public final String basePath;

    public KnoxSWebHdfsFileSystem(String basePath) {
        super();

        this.basePath = basePath;
    }

    @Override
    public synchronized void initialize(URI uri, Configuration conf) throws IOException {
        super.initialize(uri, conf);
        connectionFactory.destroy();
        int connectTimeout = (int) conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_CONNECT_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS
        );
        int readTimeout = (int) conf.getTimeDuration(
            HdfsClientConfigKeys.DFS_WEBHDFS_SOCKET_READ_TIMEOUT_KEY,
            URLConnectionFactory.DEFAULT_SOCKET_TIMEOUT,
            TimeUnit.MILLISECONDS
        );
        boolean isOAuth = conf.getBoolean(
            HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_KEY,
            HdfsClientConfigKeys.DFS_WEBHDFS_OAUTH_ENABLED_DEFAULT
        );

        ConnectionConfigurator connectionConfigurator = null;
        try {
            connectionConfigurator = new SSLConnectionConfigurator(connectTimeout, readTimeout, conf);
        } catch (GeneralSecurityException e) {
            throw new IOException(e);
        }
        if (isOAuth) {
            connectionConfigurator = new OAuth2ConnectionConfigurator(conf, connectionConfigurator);
        }
        connectionConfigurator = new BasicAuthConfigurator(connectionConfigurator, conf.get("hadoop.knox.swebhdfs.basic_auth_header", ""));

        connectionFactory = new URLConnectionFactory(connectionConfigurator);
    }

    @Override
    URL toUrl(HttpOpParam.Op op, Path fspath, Param<?, ?>... parameters) throws IOException {
        URL url = super.toUrl(op, fspath, parameters);
        String path = url.getPath();

        return new URL(url.getProtocol(), url.getHost(), url.getPort(), basePath + path + "?" + url.getQuery());
    }
}
