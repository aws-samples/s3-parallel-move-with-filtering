/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: MIT-0
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this
 * software and associated documentation files (the "Software"), to deal in the Software
 * without restriction, including without limitation the rights to use, copy, modify,
 * merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 * INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
 * PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package com.amazonaws.proserve.module;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.proserve.exception.ConfigurationErrorException;
import com.amazonaws.proserve.util.Configuration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagementClientBuilder;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AwsModule extends AbstractModule {
    private  static final Logger LOGGER = LoggerFactory.getLogger(AwsModule.class);

    @Override
    protected void configure() {}

    @Provides
    @Singleton
    AmazonS3 provideS3Client() throws Exception {
        return AmazonS3ClientBuilder.standard().withRegion(getAwsRegion()).withCredentials(new DefaultAWSCredentialsProviderChain()).build();
    }

    @Provides
    @Singleton
    TransferManager provideTransferManager(AmazonS3 s3) {
        return TransferManagerBuilder.standard().withS3Client(s3).build();
    }

    @Provides
    @Singleton
    AWSSimpleSystemsManagement provideSSMClient() throws Exception {
        return AWSSimpleSystemsManagementClientBuilder.standard().withRegion(getAwsRegion()).build();
    }

    private static AWSCredentialsProvider getProfileCredentials() {
        return new DefaultAWSCredentialsProviderChain();
    }

    private static Regions getAwsRegion() throws ConfigurationErrorException {
        return Configuration.getAwsRegion();
    }
}
