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

package com.amazonaws.proserve.s3;

import com.amazonaws.proserve.util.S3Utils;
import com.amazonaws.proserve.util.StringUtilities;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.google.inject.Inject;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class S3toS3Copier {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3toS3Copier.class);
    public final AmazonS3 s3Client;
    private final TransferManager transferManager;

    @Inject
    public S3toS3Copier(AmazonS3 s3Client, TransferManager transferManager) {
        this.s3Client=s3Client;
        this.transferManager=transferManager;
    }

    public static Boolean filterNames(S3ObjectSummary summaryA, String prefix, List<String> filterNames) {
        Boolean nameFlag = false;
        for (String name : filterNames){
            if (summaryA.getKey().contains(name)){
                nameFlag=true;
            }
        }
        if (summaryA.getKey().equals(prefix) || nameFlag){
            return false;
        }
        return true;
    }

    public static Boolean filterSizes(S3ObjectSummary summaryA, Long minFileSize, Long maxFileSize) {
        if (minFileSize == null && maxFileSize == null){
            return true;
        } else if (minFileSize != null && maxFileSize == null){
            if (summaryA.getSize() < minFileSize){
                return false;
            }
            return true;
        } else if (minFileSize == null && maxFileSize != null){
            if (summaryA.getSize() > maxFileSize){
                return false;
            }
            return true;
        } else if (minFileSize != null && maxFileSize != null){
            if (summaryA.getSize() < minFileSize || summaryA.getSize() > maxFileSize){
                return false;
            }
            return true;
        }
        return true;
    }

    public static List<S3ObjectSummary> getS3KeysWithBlacklist(AmazonS3 s3, String bucketName, String prefix, List<String> filterNames, Long minFileSize, Long maxFileSize) {

        List<S3ObjectSummary> selectedSuffixSummaryList = new ArrayList<>();

        ListObjectsV2Request request = new ListObjectsV2Request();
        request.withBucketName(bucketName);
        request.withPrefix(prefix);

        ListObjectsV2Result result = null;

        do {

            result = s3.listObjectsV2(request);
            if (Objects.isNull(result) || Objects.isNull(result.getObjectSummaries())) break;

            request.withContinuationToken(result.getNextContinuationToken());

            for(S3ObjectSummary so : result.getObjectSummaries()) {
                if (filterNames(so, prefix, filterNames) && filterSizes(so, minFileSize, maxFileSize)){
                    selectedSuffixSummaryList.add(so);
                }
            }

        } while (result.isTruncated());

        return selectedSuffixSummaryList;
    }

    public List<S3ObjectSummary> getSummariesWithBlacklist(String bucketName, String prefix, List<String> filterNames, Long minFileSize, Long maxFileSize) {
        return this.getS3KeysWithBlacklist(this.s3Client, bucketName, prefix, filterNames, minFileSize, maxFileSize);
    }

    public List<String> executeCopyWithBlacklist(String sourceBucket, String sourcePrefix, String destBucket, String destPrefix, List<String> filterNames, Long minFileSize, Long maxFileSize) {
        LOGGER.info("Source Bucket: {} Source Prefix: {} Dest Bucket: {} Dest Prefix: {}. Copying started...", sourceBucket, sourcePrefix, destBucket, destPrefix);
        //get all summaries
        List<S3ObjectSummary> summaries = this.getSummariesWithBlacklist(sourceBucket, sourcePrefix, filterNames, minFileSize, maxFileSize);
        LOGGER.info("Found {} files at source location: {}, the following files won't be moved due to filtering: {}", summaries.size(), summaries, "0");
        final StopWatch stopwatch = new StopWatch();
        stopwatch.start();
        parallelCopy(sourceBucket, sourcePrefix, destBucket, destPrefix, summaries, stopwatch);

        List<String> filesMoved = new ArrayList<>();
        for (S3ObjectSummary s : summaries){
            filesMoved.add(s.getKey());
        }
        return filesMoved;
    }

    private void parallelCopy(String sourceBucket, String sourcePrefix, String destBucket, String destPrefix, List<S3ObjectSummary> summaries, StopWatch stopwatch) {
        Map<String, String> errorMap = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() + 1);
        for (int i = 0; i < summaries.size(); i++) {
            final int finalI = i;
            executor.execute(() -> {
                copy(sourceBucket, sourcePrefix, summaries.get(finalI).getKey(), destBucket, destPrefix, errorMap);
            });
        }
        executor.shutdown();

        try {
            executor.awaitTermination(60, TimeUnit.MINUTES);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (errorMap.size() > 0) {
            LOGGER.info("---------Errors--------");
            errorMap.forEach((k, v) -> LOGGER.error("Error for {} - {}", k, v));
        }
        LOGGER.info("[Total Objects: {}]-[Failed Copy: {}]-[Time elapsed: {}]", summaries.size(), errorMap.size(), stopwatch.toString());
    }

    private boolean copy(String sourceBucket, String sourcePrefix, String sourceKey, String destBucket, String destPrefix, Map<String, String> errorMap) {
        // check if the object key does not contain the source prefix, skip
        if (StringUtilities.notNullNotEmpty(sourcePrefix) && !sourceKey.startsWith(sourcePrefix)) {
            errorMap.put(sourceKey, "Object key must start with the source prefix!");
            return false;
        }
        String sourceKeyWithoutPrefix = StringUtilities.notNullNotEmpty(sourcePrefix) ? sourceKey.replace(sourcePrefix, "") : sourceKey;
        String destKey = StringUtilities.nullOrEmpty(destPrefix) ? sourceKeyWithoutPrefix : destPrefix + sourceKeyWithoutPrefix;
        //if the object already exists, skip
        if(S3Utils.getObjectSummary(this.s3Client, destBucket, destKey) != null )
        {
            LOGGER.warn("Target object already exists. Source: {}, Target: {}",sourceKey, destKey);
            return false;
        }
        if (!S3Utils.copyBetweenS3UsingTransferManagerWithWait(this.transferManager, sourceBucket, sourceKey, destBucket, destKey, StorageClass.OneZoneInfrequentAccess)) {
            errorMap.put(sourceKey, "Copy failed");
            return false;
        }
        return true;
    }
}
