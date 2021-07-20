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

package com.amazonaws.proserve;

import com.amazonaws.proserve.controller.*;
import com.amazonaws.proserve.module.AwsModule;
import com.amazonaws.proserve.module.BaseModule;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;

import static spark.Spark.get;
import static spark.Spark.options;
import static spark.Spark.port;
import static spark.Spark.post;

public class SparkApp {

    private static final int PORT = 8080;

    private final HealthCheckController healthCheckController;
    private final AWSSimpleSystemsManagement awsSimpleSystemsManagement;

    //S3 Parallel Move With Filtering
    private final S3ParallelMoveWithFilteringController s3ParallelMoveWithFilteringController;

    @Inject
    public SparkApp(HealthCheckController healthCheckController,
                    AWSSimpleSystemsManagement awsSimpleSystemsManagement,
                    S3ParallelMoveWithFilteringController s3ParallelMoveWithFilteringController
                    ) {
    	
        this.healthCheckController = healthCheckController;
        this.awsSimpleSystemsManagement=awsSimpleSystemsManagement;
        this.s3ParallelMoveWithFilteringController=s3ParallelMoveWithFilteringController;
    }

    void run(int port) {
        port(port);
        options("/*", (request,response)->{
            String accessControlRequestHeaders = request.headers("Access-Control-Request-Headers");
            if (accessControlRequestHeaders != null) {
                response.header("Access-Control-Allow-Headers", accessControlRequestHeaders);
            }
            String accessControlRequestMethod = request.headers("Access-Control-Request-Method");
            if(accessControlRequestMethod != null){
                response.header("Access-Control-Allow-Methods", accessControlRequestMethod);
            }
            response.header("Access-Control-Allow-Origin", "*");
            return "OK";
        });

        //API calls.
        get("/health", ((request, response) -> {return  this.healthCheckController.getHealth(request, response);}));

        //Endpoints for S3 Parallel Move With Filtering ******************************************************************************************

        //S3 Move with Filtering
        post("/s3-move", ((request, response) -> {return this.s3ParallelMoveWithFilteringController.move(request, response);}));

        System.out.println("Application is running...");
    }

    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new BaseModule(), new AwsModule());
        injector.getInstance(SparkApp.class).run(PORT);
    }
}
