/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.web.KnoxSWebHdfsFileSystem;
import org.apache.hadoop.hdfs.web.SWebHdfsFileSystem;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


/**
 * Filesystem that supports scheme "knoxswebhdfs" for accessing HDFS secured by Apache Knox.
 * It is a wrapper around SWebHdfsFileSystem.
 *
 * It adds a base path to the URL, which is carried over from path part of uri.
 * Knox requires that path starts with /gateway/<gateway-name>, and regular webhdfs handlers
 * always prefix the path with /webhdfs/v1, without any way to override this.
 * Other handlers simply drop path part of the uri.
 *
 * It also adds support for basic auth header to a request.
 * Basic auth header is configured via hadoop.knox.swebhdfs.basic_auth_header property.
 */
public class KnoxSWebHdfs extends DelegateToFileSystem {

    public static final String SCHEME = "knoxswebhdfs";
    KnoxSWebHdfs(URI theUri, Configuration conf)
            throws IOException, URISyntaxException {
        super(theUri, createSWebHdfsFileSystem(theUri.getPath(), conf), conf, SCHEME, false);
    }

    private static SWebHdfsFileSystem createSWebHdfsFileSystem(String path, Configuration conf) {
        SWebHdfsFileSystem fs = new KnoxSWebHdfsFileSystem(path);
        fs.setConf(conf);
        return fs;
    }
}
