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

import com.amazonaws.services.s3.AmazonS3;
import com.google.inject.Inject;
import java.util.List;

public class MoveToTestBucket {
    public final AmazonS3 s3Client;
    private final S3toS3Copier s3Copier;

    @Inject
    public MoveToTestBucket(AmazonS3 s3Client, S3toS3Copier s3Copier) {
        this.s3Client=s3Client;
        this.s3Copier = s3Copier;
    }

    public List<String> move(String sourceBucket, String sourcePrefix, String destBucket, String destPrefix, List<String> filterNames, Long minFileSize, Long maxFileSize) {

        List<String> movedFiles = this.s3Copier.executeCopyWithBlacklist(sourceBucket, sourcePrefix, destBucket, destPrefix, filterNames, minFileSize, maxFileSize);

        return movedFiles;
    }

}
