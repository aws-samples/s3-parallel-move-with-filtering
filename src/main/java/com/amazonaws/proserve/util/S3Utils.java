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

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.proserve.exception.WriteException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.*;
import com.amazonaws.services.s3.model.lifecycle.LifecycleFilter;
import com.amazonaws.services.s3.model.lifecycle.LifecyclePrefixPredicate;
import com.amazonaws.services.s3.transfer.Copy;
import com.amazonaws.services.s3.transfer.Transfer.TransferState;
import com.amazonaws.services.s3.transfer.TransferManager;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

/**
 * Java Utility class: Set of S3 utilities
 * 
 * @author zorani
 *
 */
public final class S3Utils {

	// nothing here to instantiate
	private S3Utils() {}

	private static final Logger LOG = LoggerFactory.getLogger(S3Utils.class);

	/**
	 * Retrieve S3 object content as a String, using the S3's <code>S3Object</code> object and memory requirement
	 *  
	 * @param s3obj S3 object obtained by S3's <code>getObject</code> call
	 * @param maxMemory maximum required memory for this retrieval operation 
	 * 
	 * @return content of the S3 object as a String value
	 */
	public static String getS3ObjectContentAsString(S3Object s3obj, int maxMemory) {

		if (s3obj == null) throw new IllegalArgumentException("S3Object reference cannot be null!");
		if (maxMemory <= 0) throw new IllegalArgumentException("Specified memory size cannot be <= 0, currently specified: " + maxMemory + " bytes!");

		final int bufferSize = maxMemory;
		final char[] buffer = new char[bufferSize];
		final StringBuilder out = new StringBuilder();

		try {
			Reader in = new InputStreamReader(s3obj.getObjectContent(), "UTF-8");
			for (; ;) {
				int rsz = in.read(buffer, 0, buffer.length);
				if (rsz < 0)
					break;
				out.append(buffer, 0, rsz);
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (s3obj != null) {

				s3obj.getObjectContent().abort();

				try {
					// Close the object
					s3obj.close(); 
				} catch (IOException e) {
				}
			}
		}

		return out.toString();
	}

	/**
	 * Get the S3 object as byte[]
	 * 
	 * @param s3obj
	 * @return
	 */
	public static byte[] getS3ObjectContentAsByteArray(AmazonS3 s3, String s3BucketName, String s3ObjectKey, int MAX_MEMORY) {

		S3Object s3obj = s3.getObject(s3BucketName, s3ObjectKey);
		if (s3obj == null) throw new IllegalArgumentException("S3Object reference cannot be null!");

		int objectLength = (int)s3obj.getObjectMetadata().getContentLength();
		final byte[] data = new byte[objectLength > MAX_MEMORY ? MAX_MEMORY : objectLength + 1024];

		InputStream is = s3obj.getObjectContent();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();

		int nRead = 0;

		try {
			while ((nRead = is.read(data, 0, data.length)) != -1) {
				baos.write(data, 0, nRead);
			}

			baos.flush();
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {

			s3obj.getObjectContent().abort();

			s3obj.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		return baos.toByteArray();		
	}

	/**
	 * Retrieve S3 object content as a String, using the S3's <code>S3Object</code> object
	 * 
	 * @param s3obj S3 object obtained by S3's <code>getObject</code> call
	 * @return content of the S3 object as a String value
	 */
	public static String getS3ObjectContentAsString(S3Object s3obj) {

		// default is 1 Mb of memory buffer
		return S3Utils.getS3ObjectContentAsString(s3obj, 1024 * 1024);
	}

	/**
	 * Retrieve S3 object content as a String with provided S3 client, bucket name and object prefix
	 * 
	 * @param s3 S3 client's client reference
	 * @param bucketName S3 bucket name
	 * @param objectKey S3 object prefix
	 * @param maxMemory maximum required memory for this retrieval operation 
	 * 
	 * @return content of the S3 object as a String value
	 */
	public static String getObjectContentAsString(AmazonS3 s3, String bucketName, String objectKey, int maxMemory) {

		S3Object s3obj = s3.getObject(bucketName, objectKey);

		return S3Utils.getS3ObjectContentAsString(s3obj, maxMemory);
	}

	/**
	 * Retrieve S3 object content as a String with provided S3 client, bucket name and object prefix
	 * 
	 * @param s3 S3 client's client reference
	 * @param bucketName S3 bucket name
	 * @param objectKey S3 object prefix
	 * 
	 * @return content of the S3 object as a String value
	 */
	public static String getObjectContentAsString(AmazonS3 s3, String bucketName, String objectKey) {

		S3Object s3obj = s3.getObject(bucketName, objectKey);

		return S3Utils.getS3ObjectContentAsString(s3obj);
	}

	/**
	 * Get the content of the S3 Object as a String 
	 * 
	 * @param s3
	 * @param objectURI
	 * @return
	 */
	public static String getObjectContentAsString(AmazonS3 s3, String objectS3URL) {

		AmazonS3URI uri = new AmazonS3URI(objectS3URL);

		S3Object s3obj = s3.getObject(uri.getBucket(), uri.getKey());

		return S3Utils.getS3ObjectContentAsString(s3obj);
	}

	/**
	 * Get the content of the S3 Object as a String 
	 * 
	 * @param s3
	 * @param objectURI
	 * @return
	 */
	public static String getObjectContentAsString(AmazonS3 s3, AmazonS3URI objectURI) {

		S3Object s3obj = s3.getObject(objectURI.getBucket(), objectURI.getKey());

		return S3Utils.getS3ObjectContentAsString(s3obj);
	}


	/**
	 * Retrieve S3 object content as a String with provided AWS Region, S3 Bucket name, S3 Object prefix and max required memory
	 * 
	 * @param s3 S3 client's client reference
	 * @param bucketName S3 bucket name
	 * @param objectKey S3 object prefix
	 * @param maxMemory maximum required memory for this retrieval operation 
	 * 
	 * @return content of the S3 object as a String value
	 */
	public static String getObjectContentAsString(String s3Region, String s3BucketName, String s3ObjectKey, int maxMemory) {

		if (s3BucketName == null || s3BucketName.isEmpty()) 
			throw new IllegalArgumentException("getObjectContentAsString() - ERROR: Missing bucket name!");

		if (s3ObjectKey == null || s3ObjectKey.isEmpty()) 
			throw new IllegalArgumentException("getObjectContentAsString() - ERROR: Missing object prefix!");

		if (s3Region == null || s3Region.isEmpty()) 
			throw new IllegalArgumentException("getObjectContentAsString() - ERROR: Missing S3 region!");

		AmazonS3 s3 = null;

		try {
			s3 = AmazonS3ClientBuilder.standard()
					.withRegion(s3Region)
					.build();

			S3Object s3obj = s3.getObject(s3BucketName, s3ObjectKey);

			return S3Utils.getS3ObjectContentAsString(s3obj);

		} catch (Exception e1) {
			e1.printStackTrace();
		}

		return null;
	}

	/**
	 * Retrieve S3 object content as a String with provided AWS Region, S3 Bucket name and S3 Object prefix
	 * 
	 * @param s3 S3 client's client reference
	 * @param bucketName S3 bucket name
	 * @param objectKey S3 object prefix
	 * 
	 * @return content of the S3 object as a String value
	 */
	public static String getObjectContentAsString(String s3Region, String s3BucketName, String s3ObjectKey) {

		return S3Utils.getObjectContentAsString(s3Region, s3BucketName, s3ObjectKey, 1024 * 1024);
	}	

	/**
	 * Check if bucket exists and if it is empty [if not existing, return false]
	 * 
	 * @param s3 S3 Client reference
	 * @param bucketName S3 bucket name
	 * 
	 * @return True/False if S3 Bucket is empty
	 */
	public static boolean checkIfBucketIsEmpty(AmazonS3 s3, String bucketName) {

		// this call will return if Bucket exists and client has permission to access it
		s3.headBucket(new HeadBucketRequest(bucketName));

		//This method returns a HeadBucketResult if the bucket exists and you have permission to access it. 
		// Otherwise, the method will throw an AmazonServiceException with status code '404 Not Found' if the bucket does not exist, 
		//'403 Forbidden' if the user does not have access to the bucket, or '301 Moved Permanently' 
		//if the bucket is in a different region than the client is configured with		

		ObjectListing objListing = s3.listObjects(bucketName);
		List<S3ObjectSummary> objSummaryList = objListing.getObjectSummaries();

		for (S3ObjectSummary os : objSummaryList) {

			if (!os.getKey().endsWith("/")) return false;
		}	

		return true;
	}	

	/**
	 * Create folder in S3
	 * 
	 * @param s3
	 * @param bucketName
	 * @param folder
	 */
	public static void createFolderInS3(AmazonS3 s3, String bucketName, String folder, StorageClass storageClass) {

		createFolderInS3(s3, bucketName, folder, null, storageClass);
	}

	/**
	 * Create folder in S3
	 * 
	 * @param s3
	 * @param bucketName
	 * @param folder
	 * @param kmsId
	 */
	public static void createFolderInS3(AmazonS3 s3, String bucketName, String folder, String kmsId, StorageClass storageClass) {

		ObjectMetadata metadata = new ObjectMetadata();

		// CREATE EDL FOLDER (fileName == null)
		// TAGS are inherited from the parent folder			
		metadata.setContentLength(0);

		// create empty content
		InputStream content = new ByteArrayInputStream(new byte[0]);

		// create a PutObjectRequest passing the folder name suffixed by /
		// send request to S3 to create an object with the content from the provided template file
		if (Utils.checkIfNotNullAndNotEmptyString(kmsId)) S3Utils.putObjectWithStreamContent(s3, bucketName, folder, content, kmsId, metadata, storageClass);
		else S3Utils.putObjectWithStreamContent(s3, bucketName, folder, content, metadata, storageClass);
	}

	/**
	 * Create a bucket - if created, return TRUE. 
	 * If bucket exists, return TRUE.
	 * For any other error, return FALSE.
	 * 
	 * @param bucketName
	 * @return
	 */	
	public static boolean createBucket(AmazonS3 s3, String bucketName, boolean doNothingIfExists) {

		boolean bucketExists = false;

		// check if bucket already exists
		if (s3.doesBucketExistV2(bucketName)) {
			bucketExists = true;
		}

		if (bucketExists) {
			if (doNothingIfExists) return true;
			else return false;
		}

		// create a S3 Bucket
		Bucket bucket = s3.createBucket(bucketName);

		// enable versioning on the bucket
		SetBucketVersioningConfigurationRequest versioningRequest = new SetBucketVersioningConfigurationRequest(bucketName, new BucketVersioningConfiguration(BucketVersioningConfiguration.ENABLED));
		s3.setBucketVersioningConfiguration(versioningRequest);

		return true;
	}

	/**
	 * List all bucket entities
	 * 
	 * @param s3
	 * @param bucketName
	 * @return
	 */
	public static List<String> listBucketEntities(AmazonS3 s3, String bucketName) {

		if (!s3.doesBucketExistV2(bucketName)) return null;

		ObjectListing objListing = s3.listObjects(bucketName);
		List<S3ObjectSummary> objSummaryList = objListing.getObjectSummaries();

		boolean continueProcessing = true;
		List<String> objects = new ArrayList<String>();

		while (continueProcessing) {

			for (S3ObjectSummary os : objSummaryList) {

				objects.add(os.getKey());
			}	

			// check if more data
			if (objListing.isTruncated()) {

				objListing = s3.listNextBatchOfObjects(objListing);
			}
			else {

				continueProcessing = false;
			}
		} 

		return objects;
	}

	/**
	 * List of buckets
	 * 
	 * @param s3
	 * @return
	 */
	public static List<String> listBuckets(AmazonS3 s3) {

		List<String> listOfBucketNames = new ArrayList<>();

		List<Bucket> buckets = s3.listBuckets();
		for(Bucket bucket : buckets) {
			listOfBucketNames.add(bucket.getName());
		}

		return listOfBucketNames;
	}

	/**
	 * Get the list of object summaries for the folder with filtering by suffix
	 * 
	 * @param s3
	 * @param uri
	 * @param filterBySuffix
	 * @return
	 */
	public static List<S3ObjectSummary> getListOfObjectSummariesForFolder(AmazonS3 s3, AmazonS3URI uri, String filterBySuffix) {

		return getListOfObjectSummariesForFolder(s3, uri.getBucket(), uri.getKey(), filterBySuffix);
	}

	/**
	 * Get the list of object summaries for the folder
	 * 
	 * @param s3
	 * @param bucket
	 * @param folderKey
	 * @return
	 */
	public static List<S3ObjectSummary> getListOfObjectSummariesForFolder(AmazonS3 s3, String bucket, String prefix) {

		if (StringUtilities.notNullNotEmpty(prefix)) prefix = StringUtilities.addAtTheEndIfNotPresent(prefix.trim(), "/");

		return getListOfObjectSummariesForFolder(s3, bucket, prefix, null);
	}

	/**
	 * Get the list of object summaries for the folder
	 * 
	 * @param s3
	 * @param uri
	 * @return
	 */
	public static List<S3ObjectSummary> getListOfObjectSummariesForFolder(AmazonS3 s3, AmazonS3URI uri) {

		return getListOfObjectSummariesForFolder(s3, uri.getBucket(), uri.getKey(), null);
	}

	/**
	 * Get the list of object summaries for the folder with filtering by suffix
	 * 
	 * @param s3
	 * @param bucket
	 * @param folderKey
	 * @param filterBySuffix
	 * @return
	 */
	public static List<S3ObjectSummary> getListOfObjectSummariesForFolder(AmazonS3 s3, String bucket, String prefix, String filterBySuffix) {

		if (StringUtilities.notNullNotEmpty(prefix)) prefix = StringUtilities.addAtTheEndIfNotPresent(prefix.trim(), "/");

		ListObjectsV2Request request = new ListObjectsV2Request();
		request.setBucketName(bucket);
		request.setPrefix(prefix);
		request.withDelimiter("/");

		boolean continueProcessing = true;
		List<S3ObjectSummary> files = new ArrayList<>();

		while (continueProcessing) {

			ListObjectsV2Result list = null;
			try {
				list = s3.listObjectsV2(request);
			} catch (AmazonServiceException e) {
				e.printStackTrace();
				if (e.getErrorCode().equals("503")) {
					Utils.sleepInMilliseconds(300);
					continue;
				}
			} catch (SdkClientException e) {
				e.printStackTrace();
				return null;
			}
			
			for(S3ObjectSummary ss : list.getObjectSummaries()) {

				if (!Utils.nullOrEmpty(filterBySuffix)) {
					if (ss.getKey().toLowerCase(Locale.ENGLISH).endsWith(filterBySuffix.trim().toLowerCase(Locale.ENGLISH))) {
						files.add(ss);
					}
				}
				else {
					files.add(ss);
				}
			}

			if (Utils.checkIfNotNullAndNotEmptyString(list.getNextContinuationToken())) {
				request.withContinuationToken(list.getNextContinuationToken());
			}
			else {
				continueProcessing = false;
			}
		}

		return files;
	}

	/**
	 * Get the common prefixes from the current prefix
	 *  
	 * @param bucketName
	 * @param prefix
	 * @return
	 */
	public static List<String> getListForCommonPrefixes(AmazonS3 s3, String bucketName, String prefix) {

		if (StringUtilities.notNullNotEmpty(prefix)) prefix = StringUtilities.addAtTheEndIfNotPresent(prefix.trim(), "/");

		List<String> prefixesList = new ArrayList<>();
		ListObjectsV2Request request = new ListObjectsV2Request();
		request.withBucketName(bucketName);
		request.withPrefix(prefix);
		request.withDelimiter("/");

		ListObjectsV2Result result = null;

		LOG.info("Getting common prefixes in bucket [{}] for prefix [{}]", bucketName, prefix);
		do {

			try {
				result = s3.listObjectsV2(request);
			} catch (AmazonServiceException e) {
				e.printStackTrace();
				if (e.getErrorCode().equals("503")) {
					Utils.sleepInMilliseconds(300);
					continue;
				}
			} catch (SdkClientException e) {
				e.printStackTrace();
			}
			if (Objects.isNull(result) || Objects.isNull(result.getCommonPrefixes())) break;

			prefixesList.addAll(result.getCommonPrefixes());

			request.withContinuationToken(result.getNextContinuationToken());

		} while (result.isTruncated());

		return prefixesList;
	}

	/**
	 * Get the S3 summary objects list
	 *  
	 * @param bucketName
	 * @param prefix
	 * @return
	 */
	public static List<S3ObjectSummary> getSummaryListForChildren(AmazonS3 s3, String bucketName, String prefix) {

		if (StringUtilities.notNullNotEmpty(prefix)) prefix = StringUtilities.addAtTheEndIfNotPresent(prefix.trim(), "/");

		List<S3ObjectSummary> summaryList = new ArrayList<>();
		ListObjectsV2Request request = new ListObjectsV2Request();
		request.withBucketName(bucketName);
		request.withPrefix(prefix);

		ListObjectsV2Result result = null;

		LOG.info("Getting list of children in bucket [{}] for prefix [{}]", bucketName, prefix);
		do {

			try {
				result = s3.listObjectsV2(request);
			} catch (AmazonServiceException e) {
				e.printStackTrace();
				if (e.getErrorCode().equals("503")) {
					Utils.sleepInMilliseconds(300);
					continue;
				}
			} catch (SdkClientException e) {
				e.printStackTrace();
				return null;
			}
			
			if (Objects.isNull(result) || Objects.isNull(result.getObjectSummaries())) break;

			request.withContinuationToken(result.getNextContinuationToken());

			// change since March 11th, 2020 --> appears that S3 
			// is now returning S3ObjectSummary for folder as well
			for(S3ObjectSummary so : result.getObjectSummaries()) {
				if (so.getKey().endsWith("/")) continue;
				summaryList.add(so);
			}

			//summaryList.addAll(result.getObjectSummaries());

		} while (result.isTruncated());
		LOG.info("^ Found {} children.", summaryList.size());

		return summaryList;
	}


	/**
	 * Wrapper giving option to opt out Archived files.
	 *
	 * @param bucketName
	 * @param prefix
	 * @return
	 */
	public static List<S3ObjectSummary> getSummaryListForChildren(AmazonS3 s3, String bucketName, String prefix, boolean includeArchived) {
		List<S3ObjectSummary> result = getSummaryListForChildren(s3, bucketName, prefix);
		if(includeArchived) return result;
		else return result.stream().filter(x -> StringUtilities.notNullNotEmpty(x.getStorageClass()) && (x.getStorageClass().equalsIgnoreCase(StorageClass.IntelligentTiering.toString()) ||
				x.getStorageClass().equalsIgnoreCase(StorageClass.OneZoneInfrequentAccess.toString()) ||
				x.getStorageClass().equalsIgnoreCase(StorageClass.Standard.toString()) ||
				x.getStorageClass().equalsIgnoreCase(StorageClass.StandardInfrequentAccess.toString()) ||
				x.getStorageClass().equalsIgnoreCase(StorageClass.ReducedRedundancy.toString()))).collect(Collectors.toList());
	}
	/**
	 * Get the S3 summary objects list
	 *  
	 * @param bucketName
	 * @param prefix
	 * @return
	 */
	public static List<S3ObjectSummary> getSummaryListForChildrenAndFolders(AmazonS3 s3, String bucketName, String prefix) {

		if (StringUtilities.notNullNotEmpty(prefix)) prefix = StringUtilities.addAtTheEndIfNotPresent(prefix.trim(), "/");

		List<S3ObjectSummary> summaryList = new ArrayList<>();		
		ListObjectsV2Request request = new ListObjectsV2Request();
		request.withBucketName(bucketName);
		request.withPrefix(prefix);

		ListObjectsV2Result result = null;

		do {

			try {
				result = s3.listObjectsV2(request);
			} catch (AmazonServiceException e) {
				e.printStackTrace();
				if (e.getErrorCode().equals("503")) {
					Utils.sleepInMilliseconds(300);
					continue;
				}
				
			} catch (SdkClientException e) {
				e.printStackTrace();
				return null;
			}
			
			if (Objects.isNull(result) || Objects.isNull(result.getObjectSummaries())) break;

			request.withContinuationToken(result.getNextContinuationToken());

			// change since March 11th, 2020 --> appears that S3 
			// is now returning S3ObjectSummary for folder as well
			summaryList.addAll(result.getObjectSummaries());

		} while (result.isTruncated());

		return summaryList;
	}

	/**
	 * Get the S3 summary objects list for all keys matching given suffix (case-insensitive)
	 *  
	 * @param s3
	 * @param bucketName
	 * @param prefix
	 * @param suffix
	 * @param ignoreCase
	 * @return
	 */
	public static List<S3ObjectSummary> getAllS3KeysForGivenSuffix(AmazonS3 s3, String bucketName, String startingPrefix, String suffix, boolean ignoreCase) {

		if (StringUtilities.notNullNotEmpty(startingPrefix)) startingPrefix = StringUtilities.addAtTheEndIfNotPresent(startingPrefix.trim(), "/");

		List<S3ObjectSummary> selectedSuffixSummaryList = new ArrayList<>();
		ListObjectsV2Request request = new ListObjectsV2Request();
		request.withBucketName(bucketName);
		//if suffix is provided, use it
		if(StringUtilities.notNullNotEmpty(startingPrefix)){
			request.withPrefix(startingPrefix);
		}

		ListObjectsV2Result result = null;

		do {

			try {
				result = s3.listObjectsV2(request);
			} catch (AmazonServiceException e) {
				e.printStackTrace();
				if (e.getErrorCode().equals("503")) {
					Utils.sleepInMilliseconds(300);
					continue;
				}
			} catch (SdkClientException e) {
				e.printStackTrace();
				return null;
			}
			
			if (Objects.isNull(result) || Objects.isNull(result.getObjectSummaries())) break;

			request.withContinuationToken(result.getNextContinuationToken());

			for(S3ObjectSummary so : result.getObjectSummaries()) {
				if(ignoreCase){
					if (so.getKey().toLowerCase().endsWith(suffix.toLowerCase())) selectedSuffixSummaryList.add(so);
				}else {
					if (so.getKey().endsWith(suffix)) selectedSuffixSummaryList.add(so);
				}
			}

		} while (result.isTruncated());

		return selectedSuffixSummaryList;
	}

	/**
	 * Get the S3 summary objects list for all keys matching given suffix (case-sensitive)
	 *
	 * @param s3
	 * @param bucketName
	 * @param prefix
	 * @param suffix
	 * @param ignoreCase
	 * @return
	 */
	public static List<S3ObjectSummary> getAllS3KeysForGivenSuffix(AmazonS3 s3, String bucketName, String startingPrefix, String suffix) {

		return getAllS3KeysForGivenSuffix(s3, bucketName, startingPrefix, suffix, false);
	}

	/**
	 * Get the map of summary objects for a given suffix list
	 * 
	 * @param s3
	 * @param bucketName
	 * @param startingPrefix
	 * @param suffixSet
	 * @return
	 */
	public static Map<String, List<S3ObjectSummary>> getMapOfS3KeysForGivenSuffix(AmazonS3 s3, String bucketName, String startingPrefix, Set<String> suffixSet) {

		if (StringUtilities.notNullNotEmpty(startingPrefix)) startingPrefix = StringUtilities.addAtTheEndIfNotPresent(startingPrefix.trim(), "/");

		Map<String, List<S3ObjectSummary>> suffixSummaryMap = new HashMap<>();
		ListObjectsV2Request request = new ListObjectsV2Request();
		request.withBucketName(bucketName);
		request.withPrefix(startingPrefix);

		ListObjectsV2Result result = null;

		do {

			try {
				result = s3.listObjectsV2(request);
			} catch (AmazonServiceException e) {
				e.printStackTrace();
				if (e.getErrorCode().equals("503")) {
					Utils.sleepInMilliseconds(300);
					continue;
				}
				
			} catch (SdkClientException e) {
				e.printStackTrace();
				return null;
			}
			
			if (Objects.isNull(result) || Objects.isNull(result.getObjectSummaries())) break;

			request.withContinuationToken(result.getNextContinuationToken());

			for(S3ObjectSummary so : result.getObjectSummaries()) {

				for(String suffix : suffixSet) {

					if (so.getKey().endsWith(suffix)) {

						suffixSummaryMap.putIfAbsent(suffix, new ArrayList<>());
						suffixSummaryMap.computeIfAbsent(suffix, v -> new ArrayList<>()).add(so);
						break;
					}
				}
			}

		} while (result.isTruncated());

		return suffixSummaryMap;
	}

	/**
	 * Get the first occurrence of the object key with a given suffix
	 * 
	 * @param s3
	 * @param bucketName
	 * @param startingPrefix
	 * @param suffix
	 * @return
	 */
	public static S3ObjectSummary getFirstSummaryObjectForGivenSuffix(AmazonS3 s3, String bucketName, String startingPrefix, String suffix) {

		if (StringUtilities.notNullNotEmpty(startingPrefix)) startingPrefix = StringUtilities.addAtTheEndIfNotPresent(startingPrefix.trim(), "/");

		ListObjectsV2Request request = new ListObjectsV2Request();
		request.withBucketName(bucketName);
		request.withPrefix(startingPrefix);

		ListObjectsV2Result result = null;

		do {

			try {
				result = s3.listObjectsV2(request);
			} catch (AmazonServiceException e) {
				e.printStackTrace();
				if (e.getErrorCode().equals("503")) {
					Utils.sleepInMilliseconds(300);
					continue;
				}
			} catch (SdkClientException e) {
				e.printStackTrace();
				return null;
			}
			
			if (Objects.isNull(result) || Objects.isNull(result.getObjectSummaries())) break;

			request.withContinuationToken(result.getNextContinuationToken());

			for(S3ObjectSummary so : result.getObjectSummaries()) {
				if (so.getKey().endsWith(suffix)) return so;
			}

		} while (result.isTruncated());

		return null;
	}

	/**
	 * Test if any of the object from a given prefix is in Glacier/DeepArchive
	 * 
	 * @param s3
	 * @param bucketName
	 * @param prefix
	 * @return
	 */
	public static boolean anyObjectArchived(AmazonS3 s3, String bucketName, String prefix) {

		List<S3ObjectSummary> soList = S3Utils.getSummaryListForChildren(s3, bucketName, prefix);

		return soList.stream().anyMatch(S3Utils::isArchived);
	}

	/**
	 * Test if any of the object from a given prefix is in Glacier/DeepArchive
	 * 
	 * @param s3
	 * @param bucketName
	 * @param prefix
	 * @return
	 */
	public static boolean allObjectsArchived(AmazonS3 s3, String bucketName, String prefix) {

		List<S3ObjectSummary> soList = S3Utils.getSummaryListForChildren(s3, bucketName, prefix);

		return soList.stream().allMatch(S3Utils::isArchived);
	}

	/**
	 * Check if the provided object is in Glacier/DeepArchive
	 *
	 * @param objectSummary The S3 object summary to check
	 * @return True, if the storage class is Glacier or DeepArchive. False, otherwise.
	 */
	public static boolean isArchived(S3ObjectSummary objectSummary) {
		return "DEEP_ARCHIVE".equalsIgnoreCase(objectSummary.getStorageClass()) ||
				"GLACIER".equalsIgnoreCase(objectSummary.getStorageClass());
	}

	/**
	 * Get the Object length in a safe way
	 * 
	 * @param s3
	 * @param bucket
	 * @param objectKey
	 * @return
	 */
	public static long getTheObjectLength(AmazonS3 s3, String bucket, String objectKey) {

		if (s3 == null) throw new IllegalArgumentException("S3 reference is NULL!");
		if (bucket == null) throw new IllegalArgumentException("S3 Bucket name is missing!");
		if (objectKey == null) throw new IllegalArgumentException("S3 ObjectKey is missing!");

		S3Object s3Obj = s3.getObject(bucket, objectKey);
		long size = s3Obj.getObjectMetadata().getContentLength();
		if (s3Obj != null) {
			try {
				s3Obj.getObjectContent().abort();
				s3Obj.close();
			} catch (IOException e) {
			}
		}

		return size;
	}

	/**
	 * Get the S3 Object summary
	 * 
	 * @param s3
	 * @param bucketName
	 * @param objectKey
	 * @return
	 */
	public static S3ObjectSummary getObjectSummary(AmazonS3 s3, String bucketName, String objectKey) {

		if (s3 == null) throw new IllegalArgumentException("S3 reference is NULL!");
		if (bucketName == null) throw new IllegalArgumentException("S3 Bucket name is missing!");
		if (objectKey == null) throw new IllegalArgumentException("S3 ObjectKey is missing!");

		ListObjectsV2Request request = new ListObjectsV2Request();
		request.setBucketName(bucketName);
		request.setPrefix(objectKey);

		ListObjectsV2Result list = s3.listObjectsV2(request);
		if (list == null || list.getObjectSummaries().isEmpty()) return null;

		return list.getObjectSummaries().get(0);
	}

	/**
	 * Save String content to S3 with KMS
	 * 
	 * @param s3
	 * @param bucket
	 * @param objectKey
	 * @param content
	 * @param kmsId
	 * @return
	 */
	public static PutObjectResult putObjectWithStringContent(AmazonS3 s3, String bucket, String objectKey, String content, String kmsId, StorageClass storageClass) {

		return S3Utils.putObjectWithStringContentImpl(s3, false, bucket, objectKey, content, kmsId, storageClass);
	}

	/**
	 * Save String content to S3 with KMS
	 * 
	 * @param s3
	 * @param bucket
	 * @param objectKey
	 * @param content
	 * @param kmsId
	 * @return
	 */
	public static PutObjectResult putObjectWithStringContent(AmazonS3 s3, String bucket, String objectKey, String content, StorageClass storageClass) {

		return S3Utils.putObjectWithStringContentImpl(s3, false, bucket, objectKey, content, null, storageClass);
	}

	/**
	 * Put object in S3, using content as String
	 * 
	 * @param s3
	 * @param bucket
	 * @param objectKey
	 * @param content
	 * @return
	 */
	public static PutObjectResult putObjectWithStringContent(AmazonS3 s3, boolean testMode, String bucket, String objectKey, String content, StorageClass storageClass) {

		return S3Utils.putObjectWithStringContentImpl(s3, testMode, bucket, objectKey, content, null, storageClass);
	}

	/**
	 * Put object in S3, using content as String and KmsId for encryption
	 * 
	 * @param s3
	 * @param bucket
	 * @param objectKey
	 * @param content
	 * @param kmsId
	 * @return
	 */
	public static PutObjectResult putObjectWithStringContent(AmazonS3 s3, boolean testMode, String bucket, String objectKey, String content, String kmsId, StorageClass storageClass) {

		return S3Utils.putObjectWithStringContentImpl(s3, testMode, bucket, objectKey, content, kmsId, storageClass);
	}

	/**
	 * Implementation: Put object in S3 with a given String content, KmsId is optional
	 * 
	 * @param s3
	 * @param bucket
	 * @param objectKey
	 * @param content
	 * @param kmsId
	 * @return
	 */
	private static PutObjectResult putObjectWithStringContentImpl(AmazonS3 s3, boolean testMode, String bucket, String objectKey, String content, String kmsId, StorageClass storageClass) {

		if (testMode) return new PutObjectResult();

		byte[] fileContentBytes = content.getBytes(StandardCharsets.UTF_8);
		InputStream inputStream = new ByteArrayInputStream(fileContentBytes);

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(fileContentBytes.length);

		/*		
		StringInputStream inputStream = null;
		try {
			inputStream = new StringInputStream(content);
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		 */       
		PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, objectKey, inputStream, metadata);
		putObjectRequest.setStorageClass(storageClass);
		if (kmsId != null) putObjectRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(kmsId));

		// send request to S3 to create an object with the content from the provided template file
		return s3.putObject(putObjectRequest);
	}

	/**
	 * Save byte[] to S3
	 * 
	 * @param s3
	 * @param bucket
	 * @param objectKey
	 * @param fileContentBytes
	 * @param kmsId
	 * @return
	 */
	public static PutObjectResult putObjectWithByteArrayContent(AmazonS3 s3, String bucket, String objectKey, byte[] fileContentBytes, String kmsId, StorageClass storageClass) {

		InputStream inputStream = new ByteArrayInputStream(fileContentBytes);

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(fileContentBytes.length);

		PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, objectKey, inputStream, metadata);
		putObjectRequest.setStorageClass(storageClass);
		if (kmsId != null) putObjectRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(kmsId));

		// send request to S3 to create an object with the content from the provided template file
		return s3.putObject(putObjectRequest);
	}

	/**
	 * Put Object in S3, using Input Stream, and Metadata
	 * 
	 * @param s3
	 * @param bucket
	 * @param objectKey
	 * @param contentStream
	 * @param metadata
	 */
	public static PutObjectResult putObjectWithStreamContent(AmazonS3 s3, String bucket, String objectKey, InputStream contentStream, ObjectMetadata metadata, StorageClass storageClass) {

		return S3Utils.putObjectWithStreamContentImpl(s3, bucket, objectKey, contentStream, null, metadata, storageClass);
	}

	/**
	 * Put Object in S3, using Input Stream, Kms-Id and Metadata
	 * 
	 * @param s3
	 * @param bucket
	 * @param objectKey
	 * @param contentStream
	 * @param kmsId
	 * @param metadata
	 */
	public static PutObjectResult putObjectWithStreamContent(AmazonS3 s3, String bucket, String objectKey, InputStream contentStream, String kmsId, ObjectMetadata metadata, StorageClass storageClass) {

		return S3Utils.putObjectWithStreamContentImpl(s3, bucket, objectKey, contentStream, kmsId, metadata, storageClass);
	}

	/**
	 * Implementation: Put Object in S3, using Input Stream, Metadata and Optionally KMS-ID
	 * 
	 * @param s3
	 * @param bucket
	 * @param objectKey
	 * @param contentStream
	 * @param kmsId
	 * @param metadata
	 * @return
	 */
	private static PutObjectResult putObjectWithStreamContentImpl(AmazonS3 s3, String bucket, String objectKey, InputStream contentStream, String kmsId, ObjectMetadata metadata, StorageClass storageClass) {

		PutObjectRequest putObjectRequest = new PutObjectRequest(bucket, objectKey, contentStream, metadata);
		putObjectRequest.setStorageClass(storageClass);
		if (kmsId != null) putObjectRequest.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(kmsId));

		// send request to S3 to create an object with the content from the provided template file
		return s3.putObject(putObjectRequest);
	}

	/**
	 * Delete all objects from a given prefix
	 * 
	 * @param s3
	 * @param s3Bucket
	 * @param prefix
	 */
	public static void deleteFromPrefix(AmazonS3 s3, String s3Bucket, String prefix) {

		List<S3ObjectSummary> soList = S3Utils.getSummaryListForChildren(s3, s3Bucket, prefix);
		deleteObjects(s3, s3Bucket, prefix, soList);

		// anything left in the current folder?
		soList = S3Utils.getListOfObjectSummariesForFolder(s3, s3Bucket, prefix);
		deleteObjects(s3, s3Bucket, prefix, soList);
	}

	/**
	 * Delete objects given by the list
	 * 
	 * @param s3
	 * @param s3Bucket
	 * @param prefix
	 * @param soList
	 */
	private static void deleteObjects(AmazonS3 s3, String s3Bucket, String prefix, List<S3ObjectSummary> soList) {

		if (Utils.checkIfNotNullAndNotEmptyCollection(soList)) {

			List<DeleteObjectsRequest.KeyVersion> keys = new ArrayList<>();

			for (S3ObjectSummary so : soList) {

				DeleteObjectsRequest.KeyVersion key = new DeleteObjectsRequest.KeyVersion(so.getKey());
				keys.add(key);
			}

			DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(s3Bucket);
			deleteObjectsRequest.withKeys(keys);

			try {
				DeleteObjectsResult dor = s3.deleteObjects(deleteObjectsRequest);
			} catch (AmazonServiceException e) {
				e.printStackTrace();
				System.out.printf(" ERROR removing: [%s]%n", prefix);
			} catch (SdkClientException e) {
				e.printStackTrace();
				System.out.printf(" ERROR removing: [%s]%n", prefix);
			}
		}
	}

	/**
	 * Get the parent prefix of the given S3 key
	 * 
	 * @param key
	 * @return
	 */
	public static String getParentPrefix(String key) {

		if (Utils.checkIfNullOrEmptyString(key)) return null;

		String str = StringUtilities.removeAtTheEndIfPresent(key, "/");

		String parentKey = "";
		String[] arr = key.split("/");
		for(int i = 0; i < arr.length - 1; ++i) {
			parentKey += (arr[i] + "/");
		}

		return parentKey.isEmpty() ? "/" : parentKey;
	}

	/**
	 * Saving the various content to S3
	 * 
	 * @param s3
	 * @param bucketName
	 * @param objectKey
	 * @param content
	 */
	public static void saveFileInS3(AmazonS3 s3, String bucketName, String objectKey, String content, StorageClass storageClass) {

		int attemptCount = 1;

		do {

			try {
				S3Utils.putObjectWithStringContent(s3, bucketName, objectKey, content, storageClass);
				break;
			} catch (Exception e) {
				e.printStackTrace();
				//LOG.warn(e.getMessage());

				try {
					Thread.sleep(100 * attemptCount);
				} catch (InterruptedException e1) {
				}
			}

		} while (++attemptCount <= 10);
	}		

	/**
	 * Transfer's Manager Copy command (S3 to S3)
	 * 
	 * @param transferManager
	 * @param s3SourceBucket
	 * @param s3SourceKey
	 * @param s3TargetBucket
	 * @param s3TargetKey
	 * @param storageClass
	 * @return
	 */
	public static boolean copyBetweenS3UsingTransferManagerWithWait(TransferManager transferManager, String s3SourceBucket, String s3SourceKey, String s3TargetBucket, String s3TargetKey, StorageClass storageClass) {

		Copy copy = null;
		int attemptCount = 0;

		while (++attemptCount < 10) {

			try {
				CopyObjectRequest copyRequest = new CopyObjectRequest(s3SourceBucket, s3SourceKey, s3TargetBucket, s3TargetKey);
				if (Objects.nonNull(storageClass)) copyRequest.withStorageClass(storageClass);

				copy = transferManager.copy(copyRequest);
			} catch (AmazonClientException e) {
				e.printStackTrace();
			}

			if (Objects.nonNull(copy)) {

				TransferState ts = null;

				try {

					// block
					copy.waitForCopyResult();
					if (copy.isDone()) {
						LOG.info(String.format("Copy done: [%s]-[%s]%n", s3TargetBucket, s3TargetKey));
						return true;
					}

					do {
						ts = copy.getState();
						if (ts == TransferState.Completed) {
							LOG.info(String.format("Copy done($): [%s]-[%s]%n", s3TargetBucket, s3TargetKey));
							return true;
						}

						Utils.sleepInSeconds(1);
						LOG.info(String.format(" *** Waiting for Copy completion!"));

						// if not yet finished, loop back
					} while (ts == TransferState.InProgress);

				} catch (Exception e) {
					e.printStackTrace();
				}
			}

			LOG.warn(String.format("*** TransferManager::Copy() - Count attempted: %d%n", attemptCount));
			Utils.sleepInMilliseconds(300 * attemptCount);
		}

		LOG.error(String.format("*** TransferManager::Copy() - Copy operation has failed: [%s]-[%s]%n", s3TargetBucket, s3TargetKey));
		return false;
	}	

	/**
	 * Transfer's Manager Copy command (S3 to S3) without the WAIT
	 * 
	 * @param transferManager
	 * @param s3SourceBucket
	 * @param s3SourceKey
	 * @param s3TargetBucket
	 * @param s3TargetKey
	 * @param storageClass
	 * @return
	 */
	public static Copy copyBetweenS3UsingTransferManagerNoWait(TransferManager transferManager, String s3SourceBucket, String s3SourceKey, String s3TargetBucket, String s3TargetKey, StorageClass storageClass) {

		Copy copy = null;
		int attemptCount = 0;

		while (++attemptCount < 10) {

			try {
				CopyObjectRequest copyRequest = new CopyObjectRequest(s3SourceBucket, s3SourceKey, s3TargetBucket, s3TargetKey);
				if (Objects.nonNull(storageClass)) copyRequest.withStorageClass(storageClass);

				copy = transferManager.copy(copyRequest);
				return copy;
			} catch (AmazonClientException e) {
				e.printStackTrace();
			}

			LOG.warn(String.format("*** TransferManager::Copy() - Count attempted: %d%n", attemptCount));
			Utils.sleepInMilliseconds(300 * attemptCount);
		}

		LOG.error(String.format("*** TransferManager::Copy() - Copy operation has failed: [%s]-[%s]%n", s3TargetBucket, s3TargetKey));
		return null;
	}


	public static BucketLifecycleConfiguration.Rule  getLifecyclePolicy(String prefix, int duration, StorageClass storageClass) {

		BucketLifecycleConfiguration.Rule rule = new BucketLifecycleConfiguration.Rule()
				.withId("CRP-" + prefix)
				.withFilter(new LifecycleFilter(new LifecyclePrefixPredicate(prefix)))
				.addTransition(new BucketLifecycleConfiguration.Transition().withDays(duration).withStorageClass(storageClass))
				.withStatus(BucketLifecycleConfiguration.ENABLED);

		return rule;
	}

	public static void pushLifeCyclePolicies (String bucket, BucketLifecycleConfiguration.Rule rule, AmazonS3 s3) {


		// Add the rules to a new BucketLifecycleConfiguration.
		BucketLifecycleConfiguration configuration = s3.getBucketLifecycleConfiguration(bucket);

		if(configuration.getRules().stream().noneMatch(x -> x.getId().equalsIgnoreCase(rule.getId()))) {
			configuration.getRules().add(rule);
		}

		s3.setBucketLifecycleConfiguration(bucket, configuration);
	}

	/**
	 * Add a given tag to all objects underneath a bucket+prefix
	 *
	 * @param bucket
	 * @param prefix
	 * @param tagKey
	 * @param tagValue
	 * @param s3
	 * @throws WriteException
	 */
	public static void pushTags(String bucket, String prefix, String tagKey, String tagValue, AmazonS3 s3) throws WriteException {

		List<S3ObjectSummary> objs = S3Utils.getSummaryListForChildrenAndFolders(s3, bucket, prefix);

		if(Objects.isNull(objs) || objs.size() == 0) {
			throw new WriteException("Empty prefix.");
		}

		for(S3ObjectSummary item : objs) {
			List<Tag> newTags = new ArrayList<>();
			newTags.add(new Tag(tagKey, tagValue));

			s3.setObjectTagging(new SetObjectTaggingRequest(bucket, item.getKey(), new ObjectTagging(newTags)));
		}

	}

	/**
	 * remove all tags indiscriminately from a given bucket and prefix.
	 * @param bucket s3 bucket
	 * @param prefix s3 prefix
	 * @param s3 s3 objects
	 * @throws WriteException
	 */
	public static void removeAllTags(String bucket, String prefix, AmazonS3 s3) throws WriteException {

		List<S3ObjectSummary> objs = S3Utils.getSummaryListForChildrenAndFolders(s3, bucket, prefix);

		if(Objects.isNull(objs) || objs.size() == 0) {
			throw new WriteException("Empty prefix.");
		}

		for(S3ObjectSummary item : objs) {
			s3.deleteObjectTagging(new DeleteObjectTaggingRequest(bucket, item.getKey()));
		}

	}


	/**
	 *  Moves data from either Glacier or Glacier deep archive to desired storage class
	 * @param bucket s3 bucket
	 * @param prefix s3 prefix
	 * @param s3 s3 object
	 */
	public static void restoreFor30Days(String bucket, String prefix, AmazonS3 s3){

		List<S3ObjectSummary> objs = S3Utils.getSummaryListForChildrenAndFolders(s3, bucket, prefix);


		for(S3ObjectSummary item :objs) {
			if(!item.getStorageClass().equalsIgnoreCase(StorageClass.DeepArchive.toString()) &&
					!item.getStorageClass().equalsIgnoreCase(StorageClass.Glacier.toString())) {
				System.out.println("File" + item.getKey() + "Is not archived. Skipping.");
				continue;
			}

			RestoreObjectRequest requestRestore = new RestoreObjectRequest(item.getBucketName(), item.getKey(), 30);
			RestoreObjectResult result = s3.restoreObjectV2(requestRestore);
			s3.restoreObject(item.getBucketName(), item.getKey(), 30);


			ObjectMetadata response = s3.getObjectMetadata(bucket, item.getKey());
			Boolean restoreFlag = response.getOngoingRestore();
			System.out.format("Restoration status: %s.\n",
					restoreFlag ? "in progress" : "not in progress (finished or failed)");

			System.out.println(item.getKey() + "restored. results:" +  result.getRestoreOutputPath());


		}

	}


	/**
	 * Parallel Copy S3 folder
	 * @param s3 S3 Object
	 * @param transferManager Transfer Manager Object
	 * @param sourceBucket Source Bucket
	 * @param sourcePrefix Source Prefix to copy from
	 * @param destBucket Target Bucket
	 * @param destPrefix Target Prefix to copy to
	 * @param extension File extension to be copied (can be an empty string to copy all files)
	 * @param nameTextToBeReplaced A string to be replaced with replacementNameText in the object key (can be an empty string to disable renaming)
	 * @param replacementNameText A string to replace nameTextToBeReplaced in the object key (can be an empty string to disable renaming)
	 * @return
	 */
	public static boolean copyFolder(AmazonS3 s3, TransferManager transferManager, String sourceBucket, String sourcePrefix, String destBucket, String destPrefix, String extension, String nameTextToBeReplaced, String replacementNameText) {
		LOG.info("Performing S3 Folder copy. Source Bucket: {} Source Prefix: {} Dest Bucket: {} Dest Prefix: {}. Copying started...", sourceBucket, sourcePrefix, destBucket, destPrefix);
		//get all summaries
		List<S3ObjectSummary> summaries = getAllS3KeysForGivenSuffix(s3, sourceBucket, sourcePrefix, extension, true);
		LOG.info("Found {} {} files...", summaries.size(), extension);
		final StopWatch stopwatch = new StopWatch();
		stopwatch.start();
		return parallelCopy(s3, transferManager, sourceBucket, sourcePrefix, destBucket, destPrefix, summaries, nameTextToBeReplaced, replacementNameText, stopwatch);
	}

	private static boolean parallelCopy(AmazonS3 s3, TransferManager transferManager, String sourceBucket, String sourcePrefix, String destBucket, String destPrefix, List<S3ObjectSummary> summaries,  String nameTextToBeReplaced, String replacementNameText, StopWatch stopwatch) {
		Map<String, String> errorMap = new ConcurrentHashMap<>();
		ExecutorService executor = Executors.newFixedThreadPool(
				Runtime.getRuntime().availableProcessors() + 1);
		for (int i = 0; i < summaries.size(); i++) {
			final int finalI = i;
			executor.execute(() -> {
				copyObject(s3, transferManager, sourceBucket, sourcePrefix, summaries.get(finalI).getKey(), destBucket, destPrefix, nameTextToBeReplaced, replacementNameText, errorMap);
			});
		}
		executor.shutdown();

		try {
			executor.awaitTermination(60, TimeUnit.MINUTES);

		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
		int errorsCount = errorMap.size();
		if (errorsCount > 0) {
			LOG.info("---------Errors--------");
			errorMap.forEach((k, v) -> LOG.error("Error for {} - {}", k, v));
		}
		LOG.info("[Total Objects: {}]-[Failed Copy: {}]-[Time elapsed: {}]", summaries.size(), errorMap.size(), stopwatch.toString());
		return errorsCount == 0;
	}

	private static boolean copyObject(AmazonS3 s3, TransferManager transferManager, String sourceBucket, String sourcePrefix, String sourceKey, String destBucket, String destPrefix, String nameTextToBeReplaced, String replacementNameText, Map<String, String> errorMap) {
		// check if the object key does not contain the source prefix, skip
		if (StringUtilities.notNullNotEmpty(sourcePrefix) && !sourceKey.startsWith(sourcePrefix)) {
			errorMap.put(sourceKey, "Object key must start with the source prefix!");
			return false;
		}
		//make sure the source prefix ends with /
		String updatedSrcPrefix = sourcePrefix;
		if (StringUtilities.notNullNotEmpty(sourcePrefix) && sourcePrefix.charAt(sourcePrefix.length() - 1) != '/' ){
			updatedSrcPrefix += "/";
		}
		String updatedDestPrefix = destPrefix;
		if(StringUtilities.notNullNotEmpty(destPrefix) && destPrefix.charAt(destPrefix.length() - 1) != '/' ){
			updatedDestPrefix += "/";
		}
		String sourceKeyWithoutPrefix = StringUtilities.notNullNotEmpty(sourcePrefix) ? sourceKey.replace(updatedSrcPrefix, "") : sourceKey;
		String destKey = StringUtilities.nullOrEmpty(destPrefix) ? sourceKeyWithoutPrefix : updatedDestPrefix + sourceKeyWithoutPrefix;
		// check if text replacement is required
		if(StringUtilities.notNullNotEmpty(nameTextToBeReplaced) && StringUtilities.notNullNotEmpty(replacementNameText)){
			destKey = destKey.replace(nameTextToBeReplaced, replacementNameText);
			// also replace lowercase text
			destKey = destKey.replace(nameTextToBeReplaced.toLowerCase(), replacementNameText.toLowerCase());
		}
		//if the object already exists, skip
		S3ObjectSummary sourceSummary = getObjectSummary(s3, sourceBucket, sourceKey);
		if(getObjectSummary(s3, destBucket, destKey) != null )
		{
			LOG.warn("Target object already exists. Source: {}, Target: {}",sourceKey, destKey);
			return false;
		}
		if (!copyBetweenS3UsingTransferManagerWithWait(transferManager, sourceBucket, sourceKey, destBucket, destKey, StorageClass.fromValue(sourceSummary.getStorageClass()))) {
			errorMap.put(sourceKey, "Copy failed");
			return false;
		}
		return true;
	}



} // end S3Utils