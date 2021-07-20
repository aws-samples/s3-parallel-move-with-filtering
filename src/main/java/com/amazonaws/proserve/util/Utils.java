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

import java.util.Collection;

/**
 * @author zorani
 *
 */
public class Utils {

	/**
	 * Check if NULL or EMPTY string
	 * 
	 * @param strToValidate
	 * @return
	 */
	public static boolean nullOrEmpty(String strToValidate) {
		return strToValidate == null || strToValidate.isEmpty();
	}

	/**
	 * Check if the string is Null or Empty
	 * 
	 * @param strToValidate
	 * @return
	 */
	public static boolean checkIfNullOrEmptyString(String strToValidate) {
		return Utils.nullOrEmpty(strToValidate);
	}

	/**
	 * Throw exception if the object is NULL
	 * 
	 * @param o
	 * @param exceptionMessage
	 */
	public static void throwIfNullObject(Object o, String exceptionMessage) {
		if (o == null) throw new IllegalArgumentException(exceptionMessage);
	}

	/**
	 * Check if not NULL and not Empty string
	 * 
	 * @param strToValidate
	 * @return
	 */
	public static boolean checkIfNotNullAndNotEmptyString(String strToValidate) {
		return strToValidate != null && !strToValidate.isEmpty();
	}

	/**
	 * Throw exception if this an empty String
	 * 
	 * @param strToValidate
	 * @param exceptionMessage
	 */
	public static void throwIfNullOrEmptyString(String strToValidate, String exceptionMessage) {
		if (strToValidate == null || strToValidate.isEmpty()) throw new IllegalArgumentException(exceptionMessage);
	}

	/**
	 * Check if collection exists and it is not empty
	 * @param c
	 * @return
	 */
	public static boolean checkIfNotNullAndNotEmptyCollection(Collection c) {
		if (c != null && !c.isEmpty()) return true;
		return false;
	}
	
	/**
	 * Check if collection is NULL or empty
	 * 
	 * @param c
	 * @return
	 */
	public static boolean checkIfNullOrEmptyCollection(Collection c) {
		if (c == null || c.isEmpty()) return true;
		return false;
	}
	
	/**
	 * Sleep (parameter - sleep time in seconds)
	 * 
	 * @param seconds
	 */
	public static void sleepInSeconds(int seconds) {
		
		try {
			Thread.sleep(seconds * 1000L);
		} catch (InterruptedException e) {
			//e.printStackTrace();
			
			System.out.println("WARNING: Sleep() is interrupted!");
			Thread.currentThread().interrupt();
		}
	}

	/**
	 * Sleep (parameter - sleep time in milliseconds)
	 *
	 * @param seconds
	 */
	public static void sleepInMilliseconds(long milliseconds) {

		try {
			Thread.sleep(milliseconds);
		} catch (InterruptedException e) {

			//e.printStackTrace();
			System.out.println("WARNING: Sleep() is interrupted!");
			Thread.currentThread().interrupt();
		}
	}
	
} // end Utils