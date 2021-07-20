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

import com.amazonaws.proserve.types.rest.REPLY;

import spark.Request;
import spark.Response;

public abstract class BaseController {
    protected void setHealthyResponse(Response response) {
        response.status(REPLY.OK.getValue());
        response.type("application/json");
    }

    protected void setErrorResponse(Response response) {
        response.status(REPLY.HTTP_BAD_REQ.getValue());
    }

    protected  void setHeaders(Request request, Response response) {
        String accessControlRequestHeaders = request
                .headers("Access-Control-Request-Headers");
        if (accessControlRequestHeaders != null) {
            response.header("Access-Control-Allow-Headers",
                    accessControlRequestHeaders);
        }

        String accessControlRequestMethod = request
                .headers("Access-Control-Request-Method");
        if (accessControlRequestMethod != null) {
            response.header("Access-Control-Allow-Methods",
                    accessControlRequestMethod);
        }

        response.header("Access-Control-Allow-Origin", "*");
    }
}
