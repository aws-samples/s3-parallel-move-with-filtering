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


import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.Objects;

/**
 * String utilities
 * 
 * @author zorani
 */
public class StringUtilities {

	/**
	 * Remove from the start of the string
	 * 
	 * @param removeFrom
	 * @param removeString
	 * @return
	 */
	public static String removeFromTheFrontIfPresent(String removeFrom, String prefixToRemove) {
		
		if (Objects.isNull(removeFrom)) return null;
		if (Utils.checkIfNullOrEmptyString(prefixToRemove)) return removeFrom;
		
		if (!removeFrom.startsWith(prefixToRemove)) return removeFrom;
		
		return removeFrom.substring(prefixToRemove.length());
	}

	/**
	 * Remove at the end of the given string
	 * 
	 * @param removeFrom
	 * @param removeString
	 * @return
	 */
	public static String removeAtTheEndIfPresent(String removeFrom, String removeString) {
		
		if (Objects.isNull(removeFrom)) return null;
		if (Utils.checkIfNullOrEmptyString(removeString)) return removeFrom;
		
		if (!removeFrom.endsWith(removeString)) return removeFrom;
		
		return removeFrom.substring(0, removeFrom.length() - removeString.length());
	}
	
	/**
	 * Add the subString at the beginning of the string, if not already present
	 * 
	 * @param originalString
	 * @param addString
	 * @return
	 */
	public static String addAtTheStartIfNotPresent(String originalString, String addString) {
		
		if (Objects.isNull(originalString)) return null;
		if (Utils.checkIfNullOrEmptyString(addString)) return originalString;
		
		if (originalString.startsWith(addString)) return originalString;
		
		return addString + originalString;
	}
	
	/**
	 * Add the subString at the end of the string, if not already present
	 * 
	 * @param originalString
	 * @param addString
	 * @return
	 */
	public static String addAtTheEndIfNotPresent(String originalString, String addString) {
		
		if (Objects.isNull(originalString)) return null;
		if (Utils.checkIfNullOrEmptyString(addString)) return originalString;
		
		if (originalString.endsWith(addString)) return originalString;
		
		return originalString + addString;
	}
	
	/**
	 * Remove extra (repeating) white space characters
	 * 
	 * @param original
	 * @return
	 */
	public static String removeExcessiveWhiteSpace(String original) {
		
		if (Utils.checkIfNullOrEmptyString(original)) return original;
		
		StringBuilder sb = new StringBuilder();
		
		boolean prevWC = false;
		for(int i = 0; i < original.length(); ++i) {
			
			if (Character.isWhitespace(original.charAt(i))) {
				if (prevWC) continue;
				prevWC = true;
			}
			else {
				prevWC = false;
			}
			
			sb.append(original.charAt(i));
		}
		
		return sb.toString();
	}

	/**
	 * Remove repeating characters
	 * 
	 * @param originalString
	 * @param charToRemove
	 * @return
	 */
	public static String removeRepeatingCharacterFromStart(String originalString, char charToRemove) {
		
		if (Objects.isNull(originalString)) return null;
		
		int newStart = 0;
		for(int i = 0; i < originalString.length(); ++i) {
			if (charToRemove != originalString.charAt(i)) break;
			
			++newStart;
		}

		return originalString.substring(newStart);
	}
	
    /**
     * Get the last segment
     * 
     * @param original
     * @param delimiter
     * @return
     */
    public static String getLastSegment(String original, String delimiter) {
    	
        if (Utils.checkIfNullOrEmptyString(original) || Utils.checkIfNullOrEmptyString(delimiter)) return original;
        
        String[] arr = original.split(delimiter);
        if (arr == null || arr.length < 2) return original;

        return arr[arr.length - 1];
    }
    
    /**
     * Remove white spaces at the end
     * 
     * @param original
     * @return
     */
    public static String removeWhiteSpaceAtTheEnd(String original) {
    	
        if (Utils.checkIfNullOrEmptyString(original)) return original;
        
        char[] arr = original.toCharArray();
        
        int shrinkAmount = 0;
        for(int i = original.length() - 1; i >= 0; --i) {
        	if (Character.isWhitespace(arr[i])) {
        		arr[i] = ' ';
        		++shrinkAmount;
        	}
        	else {
        		break;
        	}
        }
        
        return original.substring(0, original.length() - shrinkAmount);
    }
    
    /**
     * Remove white space form the fron of the string
     * 
     * @param original
     * @return
     */
    public static String removeWhiteSpaceFromTheFront(String original) {
    	
        if (Utils.checkIfNullOrEmptyString(original)) return original;
        
        char[] arr = original.toCharArray();
        
        int shrinkAmount = 0;
        for(int i = 0; i < original.length(); ++i) {
        	if (Character.isWhitespace(arr[i])) {
        		arr[i] = ' ';
        		++shrinkAmount;
        	}
        	else {
        		break;
        	}
        }
        
        return original.substring(shrinkAmount);
    }    

    /**
     * Check if the string contains only digits
     * 
     * @param str
     * @return
     */
	public static boolean containsOnlyDigits(String str) {
		
		if (Utils.checkIfNullOrEmptyString(str)) return false;
		
		for(int i = 0; i < str.length(); ++i) {
			if (!Character.isDigit(str.charAt(i))) return false;
		}
		
		return true;
	}

	/**
	 * Convert 'in ( x, y,   z)' to 'in(x,y,z)'
	 *  
	 * @param original
	 * @return
	 */
	public static String removeAnyWhitespace(String original) {
		
        if (Utils.checkIfNullOrEmptyString(original)) return original;
        
        char[] arr = original.toCharArray();
        char[] newArray = new char[arr.length];
        
        int k = 0;
        for(int i = 0; i < arr.length; ++i) {

        	if (!Character.isWhitespace(arr[i])) {
        		newArray[k++] = arr[i];
        	}
        }
        
        return new String(newArray, 0, k);
	}
	
	/**
	 * Pad the string at the end; n - required string length after padding
	 * 
	 * @param s
	 * @param n
	 * @param paddingChar
	 * @return
	 */
	public static String padRightToGivenSize(String s, int n, char paddingChar) {
		
		if (Objects.isNull(s)) return s;
		
		if (n == 0) return "";
		if (n < 0) return null;
		if (n < s.length()) return s.substring(0, n);
		
		String tmp = s + String.join("", Collections.nCopies(n, String.valueOf(paddingChar)));
		//return String.format("%-" + n + "s", s).replace(' ', paddingChar);
		
		return tmp.substring(0, n);
	}

	/**
	 * Pad the string from the left
	 * 
	 * @param s
	 * @param n
	 * @param paddingChar
	 * @return
	 */
	public static String padLeftToGivenSize(String s, int n, char paddingChar) {

		if (Objects.isNull(s)) return s;
		
		if (n == 0) return "";
		if (n < 0) return null;
		if (n < s.length()) return s.substring(0, n);
		
		String tmp = String.join("", Collections.nCopies(n - s.length(), String.valueOf(paddingChar))) + s;
		//return String.format("%" + n + "s", s).replace(' ', paddingChar);
		
		return tmp.substring(0, n);
	}
	
	
	// ------------------
	
	/**
	 * Check if the string is Null or Empty
	 * 
	 * @param strToValidate
	 * @return
	 */
	public static boolean nullOrEmpty(String s) {
		return Objects.isNull(s) || s.isEmpty();
	}

	/**
	 * Check if the object is NULL
	 * 
	 * @param o
	 * @return
	 */
	public static boolean isNull(String s) {
		return Objects.isNull(s);
	}

	/**
	 * Check if not null
	 * 
	 * @param o
	 * @return
	 */
	public static boolean notNull(String s) {
		return Objects.nonNull(s);
	}

	/**
	 * Check if not NULL and not Empty string
	 * 
	 * @param s
	 * @return
	 */
	public static boolean notNullNotEmpty(String s) {
		return Objects.nonNull(s) && !s.isEmpty();
	}
}