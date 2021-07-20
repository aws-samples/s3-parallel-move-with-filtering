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

package com.amazonaws.proserve.util;

import com.amazonaws.proserve.exception.ConfigurationErrorException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.simplesystemsmanagement.AWSSimpleSystemsManagement;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterRequest;
import com.amazonaws.services.simplesystemsmanagement.model.GetParameterResult;
import com.amazonaws.services.simplesystemsmanagement.model.ParameterNotFoundException;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.math.BigDecimal;
import java.util.*;

/**
 * Configuration
 * 
 * @author zorani
 */
public class Configuration {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(Configuration.class);

	private static final String AWS_PROFILE = "AWS_PROFILE";
    private static final String AWS_REGION = "AWS_REGION";
	public static final StorageClass S3_STORAGE_CLASS = StorageClass.OneZoneInfrequentAccess;

	public static long KB = 1024;
	public static long MB = 1024 * 1024;
	public static long GB = 1024 * 1024 * 1024;

	public final AWSSimpleSystemsManagement ssmanager;
    
    @Inject
    public Configuration(AWSSimpleSystemsManagement awsSimpleSystemsManagement) {
        this.ssmanager = awsSimpleSystemsManagement;
    }

    /**
     * Get the AWS Profile name value
     * @return
     */
    public static String getAwsProfile() {
        String profileName = System.getenv(AWS_PROFILE);
        if(profileName == null) {
            LOGGER.error("Cannot find profile name in environment variables: " + AWS_PROFILE);
        }
        return profileName;
    }

    /**
     * Get the AWS region
     * @return
     * @throws ConfigurationErrorException
     */
    public static Regions getAwsRegion() throws ConfigurationErrorException {
    	
        String region = System.getenv(AWS_REGION);

        if(region == null) {
            LOGGER.error("Cannot find aws region in environment variables: " + AWS_REGION);
            throw new ConfigurationErrorException("AWS Region required in environment variables.");
        }
        try {
            return Regions.fromName(region);
        } catch(IllegalArgumentException e) {
            throw new ConfigurationErrorException(e.getMessage());
        }
    }

	/**
     * Retrieve parameter value
     * 
     * @param name
     * @return
     */
    public Optional<String> getListOfValues(String name) {
    	
    	if (Objects.isNull(this.ssmanager)) return Optional.empty();
    	
    	GetParameterResult result = null;
		
    	try {
			result = this.ssmanager.getParameter(new GetParameterRequest().withName(name));
			if (Objects.isNull(result)) return Optional.empty();

			return Optional.ofNullable(result.getParameter().getValue());
		} catch (ParameterNotFoundException e1) {
			
		} catch (Exception e) {
			e.printStackTrace();
		}
    	
		return Optional.empty();
    }

    /**
     * Retrieve numeric value
     * 
     * @param paramName
     * @return
     */
    public Optional<BigDecimal> getNumericValue(String paramName) {
    	
    	try {
			GetParameterResult result = this.ssmanager.getParameter(new GetParameterRequest().withName(paramName));
			if (Objects.isNull(result)) return Optional.empty();
			
			return Optional.of(new BigDecimal(result.getParameter().getValue()));
			
		} catch (ParameterNotFoundException e1) {			
			
		} catch (Exception e) {
		}

    	return Optional.empty();
    }
    
    /**
     * Retrieve string value
     * 
     * @param paramName
     * @return
     */
    public Optional<String> getStringValue(String paramName) {
    	
    	try {
			GetParameterResult result = this.ssmanager.getParameter(new GetParameterRequest().withName(paramName));
			if (Objects.isNull(result)) return Optional.empty();
			
			return Optional.of(result.getParameter().getValue());

    	} catch (ParameterNotFoundException e1) {

		} catch (Exception e) {
		}

    	return Optional.empty();
    }
} 