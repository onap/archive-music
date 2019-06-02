/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 * 
 * ============LICENSE_END=============================================
 * ====================================================================
 */

package org.onap.music.authentication;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;
import org.onap.aaf.cadi.CadiWrap;
import org.onap.aaf.cadi.Permission;
import org.onap.aaf.cadi.aaf.AAFPermission;
import org.onap.music.eelf.logging.EELFLoggerDelegate;

public class AuthUtil {

    private static final String decodeValueOfForwardSlash = "2f";
    private static final String decodeValueOfHyphen = "2d";
    private static final String decodeValueOfAsterisk = "2a";
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(AuthUtil.class);

    /**
     * Get the list of permissions from the Request object.
     * 
     *  
     * @param request servlet request object
     * @return returns list of AAFPermission of the requested MechId for all the
     *         namespaces
     */
    public static List<AAFPermission> getAAFPermissions(ServletRequest request) {
        CadiWrap wrapReq = (CadiWrap) request;

        List<Permission> perms = wrapReq.getPermissions(wrapReq.getUserPrincipal());
        List<AAFPermission> aafPermsList = new ArrayList<>();
        for (Permission perm : perms) {
            AAFPermission aafPerm = (AAFPermission) perm;
            aafPermsList.add(aafPerm);
        }
        return aafPermsList;
    }

    /**
     * Here is a sample of a permission object in AAI. The key attribute will have 
     * Type|Instance|Action.
     * AAFPermission:
     *   NS: null
     *   Type: org.onap.music.cadi.keyspace ( Permission Type )
     *   Instance: tomtest   ( Cassandra Keyspace )
     *   Action: *|GET|ALL   ( Access Level [*|ALL] for full access and [GET] for Read only)
     *   Key: org.onap.music.cadi.keyspace|tomtest|*
     *   
     * This method will filter all permissions whose key starts with the requested namespace. 
     * The nsamespace here is the music namespace which is defined in music.property file.
     * i;e is the type contains in key is org.onap.music.cadi.keyspace and the namespace 
     * value is org.onap.music.cadi.keyspace, it will add to list
     * otherwise reject.
     * 
     * @param nameSpace
     * @param allPermissionsList
     * @return
     */
    private static List<AAFPermission> filterNameSpacesAAFPermissions(String nameSpace,
            List<AAFPermission> allPermissionsList) {
        List<AAFPermission> list = new ArrayList<>();
        for (Iterator iterator = allPermissionsList.iterator(); iterator.hasNext();) {
            AAFPermission aafPermission = (AAFPermission) iterator.next();
            if(aafPermission.getType().indexOf(nameSpace) == 0) {
                list.add(aafPermission);
            }
        }
        return list;
    }

    /**
     * Decode certian characters from url encoded to normal.
     * 
     * @param str - String being decoded.
     * @return returns the decoded string.
     * @throws Exception throws excpetion
     */
    public static String decodeFunctionCode(String str) throws Exception {
        String decodedString = str;
        List<Pattern> decodingList = new ArrayList<>();
        decodingList.add(Pattern.compile(decodeValueOfForwardSlash));
        decodingList.add(Pattern.compile(decodeValueOfHyphen));
        decodingList.add(Pattern.compile(decodeValueOfAsterisk));
        for (Pattern xssInputPattern : decodingList) {
            try {
                decodedString = decodedString.replaceAll("%" + xssInputPattern,
                        new String(Hex.decodeHex(xssInputPattern.toString().toCharArray())));
            } catch (DecoderException e) {
                logger.error(EELFLoggerDelegate.applicationLogger, 
                    "AuthUtil Decode Failed! for instance: " + str);
                throw new Exception("decode failed", e);
            }
        }

        return decodedString;
    }

    /**
     * 
     * 
     * @param request servlet request object
     * @param nameSpace application namespace
     * @return boolean value if the access is allowed
     * @throws Exception throws exception
     */
    public static boolean isAccessAllowed(ServletRequest request, String nameSpace) throws Exception {

        if (request==null) {
            throw new Exception("Request cannot be null");
        }
        
        if (nameSpace==null || nameSpace.isEmpty()) {
            throw new Exception("NameSpace not Declared!");
        }
        
        boolean isauthorized = false;
        List<AAFPermission> aafPermsList = getAAFPermissions(request);
        //logger.info(EELFLoggerDelegate.applicationLogger,
        //        "AAFPermission  of the requested MechId for all the namespaces: " + aafPermsList);

        logger.debug(EELFLoggerDelegate.applicationLogger, "Requested nameSpace: " + nameSpace);


        List<AAFPermission> aafPermsFinalList = filterNameSpacesAAFPermissions(nameSpace, aafPermsList);

        logger.debug(EELFLoggerDelegate.applicationLogger,
            "AuthUtil list of AAFPermission for the specific namespace ::::::::::::::::::::::::::::::::::::::::::::"
            + aafPermsFinalList);
        
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String requestUri = httpRequest.getRequestURI().substring(httpRequest.getContextPath().length() + 1);

        logger.debug(EELFLoggerDelegate.applicationLogger,
                "AuthUtil requestUri ::::::::::::::::::::::::::::::::::::::::::::" + requestUri);

        for (Iterator iterator = aafPermsFinalList.iterator(); iterator.hasNext();) {
            AAFPermission aafPermission = (AAFPermission) iterator.next();
            if(!isauthorized) {
                isauthorized = isMatchPatternWithInstanceAndAction(aafPermission, requestUri, httpRequest.getMethod());
            }
        }
        
        logger.debug(EELFLoggerDelegate.applicationLogger,
            "isAccessAllowed for the request uri: " + requestUri + "is :" + isauthorized);
        return isauthorized;
    }

    /**
     * 
     * This method will check, if the requested URI matches any of the instance 
     * found with the AAF permission list.
     * i;e if the request URI is; /v2/keyspaces/tomtest/tables/emp15 and in the 
     * AAF permission table, we have an instance 
     * defined as "tomtest" mapped the logged in user, it will allow else error.
     * 
     * User trying to create or aquire a lock
     * Here is the requested URI /v2/locks/create/tomtest.MyTable.Field1
     * Here the keyspace name i;e tomtest will be test throught out the URL if it 
     * matches, it will allow the user to create a lock.
     * "tomtest" here is the key, which is mapped as an instance in permission object.
     * Instance can be delimited with ":" i;e ":music-cassandra-1908-dev:admin". In this case, 
     * each delimited
     * token will be matched with that of request URI.
     * 
     * Example Permission:
     * org.onap.music.api.user.access|tomtest|* or ALL
     * org.onap.music.api.user.access|tomtest|GET
     * In case of the action field is ALL and *, user will be allowed else it will 
     * be matched with the requested http method type.
     * 
     * 
     * 
     * @param aafPermission - AAfpermission obtained by cadi.
     * @param requestUri - Rest URL client is calling.
     * @param method - REST Method being used (GET,POST,PUT,DELETE)
     * @return returns a boolean
     * @throws Exception - throws an exception
     */
    private static boolean isMatchPatternWithInstanceAndAction(
        AAFPermission aafPermission, 
        String requestUri, 
        String method) throws Exception {
        if (null == aafPermission || null == requestUri || null == method) {
            return false;
        }

        String permKey = aafPermission.getKey();
        
        logger.info(EELFLoggerDelegate.applicationLogger, "isMatchPattern permKey: " 
            + permKey + ", requestUri " + requestUri + " ," + method);
        
        String[] keyArray = permKey.split("\\|");
        String[] subPath = null;
        //String type = null;
        //type = keyArray[0];
        String instance = keyArray[1];
        String action = keyArray[2];
        
        //if the instance & action both are * , then allow
        if ("*".equalsIgnoreCase(instance) && "*".equalsIgnoreCase(action)) {
            return true;
        }
        //Decode string like %2f, %2d and %2a
        if (!"*".equals(instance)) {
            instance = decodeFunctionCode(instance);
        }
        if (!"*".equals(action)) {
            action = decodeFunctionCode(action);
        }
        //Instance: :music-cassandra-1908-dev:admin
        List<String> instanceList = Arrays.asList(instance.split(":"));
        
        String[] path = requestUri.split("/");
        
        for (int i = 0; i < path.length; i++) {
            // Sometimes the value will begin with "$", so we need to remove it
            if (path[i].startsWith("$")) {
                path[i] = path[i].replace("$","");
            }
            // Each path element can again delemited by ".";i;e 
            // tomtest.tables.emp. We have scenarios like lock aquire URL
            subPath = path[i].split("\\.");
            for (int j = 0; j < subPath.length; j++) {
                if (instanceList.contains(subPath[j])) {
                    if ("*".equals(action) || "ALL".equalsIgnoreCase(action)) {
                        return true;
                    } else if (method.equalsIgnoreCase(action)) {
                        return true;
                    } else {
                        return false;
                    }
                } else {
                    continue;
                }
            }
        }
        return false;
    }
}