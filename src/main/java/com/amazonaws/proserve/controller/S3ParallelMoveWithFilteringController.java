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

package com.amazonaws.proserve.controller;

import com.amazonaws.proserve.s3.MoveToTestBucket;
import com.amazonaws.proserve.types.gateway.move.S3ParallelMoveWithFilteringResponse;
import com.amazonaws.services.s3.AmazonS3;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.Request;
import spark.Response;
import java.util.*;

public class S3ParallelMoveWithFilteringController extends BaseController {
    private static final Logger LOGGER = LoggerFactory.getLogger(S3ParallelMoveWithFilteringController.class);
    private final ObjectMapper mapper;
    public final AmazonS3 s3Client;
    private final MoveToTestBucket moveToTestBucket;

    @Inject
    public S3ParallelMoveWithFilteringController(ObjectMapper mapper,
                                                 AmazonS3 s3Client,
                                                 MoveToTestBucket moveToTestBucket) {
        this.mapper = mapper;
        this.s3Client=s3Client;
        this.moveToTestBucket = moveToTestBucket;
    }

    public String move(Request request, Response response) {
        String output = "";

        try {
            S3MoveRequest job = mapper.readValue(request.body(), S3MoveRequest.class);
            output = this.moveNoRequest(job.getS3SourceBucket(), job.getS3SourcePrefix(), job.getS3TargetBucket(), job.getS3TargetPrefix(), job.getFilterNames(), job.getMinFileSize(), job.getMaxFileSize());
        } catch (JsonProcessingException e){
            return "Unable to parse request body";
        }

        setHealthyResponse(response);
        return output;
    }

    public String moveNoRequest(String s3SourceBucket, String s3SourcePrefix, String s3TargetBucket, String s3TargetPrefix, List<String> filterNames, Long minFileSize, Long maxFileSize) {
        LOGGER.debug("Parallel s3 move requested.");

        //Move source s3 objects to target location with filtering
        List<String> movedFiles = this.moveToTestBucket.move(s3SourceBucket,s3SourcePrefix,s3TargetBucket,s3TargetPrefix,filterNames,minFileSize,maxFileSize);
        S3ParallelMoveWithFilteringResponse moveResponse = S3ParallelMoveWithFilteringResponse.builder().successfulMove("True").movedFiles(movedFiles).build();

        System.out.println("S3 Parallel Move With Filtering complete!");

        try{
            return mapper.writeValueAsString(moveResponse);
        } catch (JsonProcessingException e) {
            return "Response Output Error";
        }
    }
}

@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
class S3MoveRequest {
    @JsonProperty("s3-source-bucket")
    private String s3SourceBucket;

    @JsonProperty("s3-source-prefix")
    private String s3SourcePrefix;

    @JsonProperty("s3-target-bucket")
    private String s3TargetBucket;

    @JsonProperty("s3-target-prefix")
    private String s3TargetPrefix;

    @JsonProperty("filter-names")
    private List<String> filterNames;

    @JsonProperty("filter-min-file-size")
    private Long minFileSize;

    @JsonProperty("filter-max-file-size")
    private Long maxFileSize;
}