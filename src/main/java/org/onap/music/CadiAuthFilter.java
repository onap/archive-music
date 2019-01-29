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

package org.onap.music;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.onap.aaf.cadi.Access;
import org.onap.aaf.cadi.CadiWrap;
import org.onap.aaf.cadi.Permission;
import org.onap.aaf.cadi.PropAccess;
import org.onap.aaf.cadi.aaf.AAFPermission;
import org.onap.aaf.cadi.filter.CadiFilter;

public class CadiAuthFilter extends CadiFilter {

    private static String include_url_endpoints ="";
    private static String exclude_url_endpoints = "";
    public static final String AUTHORIZATION = "Authorization";
    
    public CadiAuthFilter(boolean init, PropAccess access) throws ServletException {
        super(true, access);
    }
    
    public void init(FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
        include_url_endpoints = filterConfig.getInitParameter("include_url_endpoints");
        exclude_url_endpoints = filterConfig.getInitParameter("exclude_url_endpoints");
    }


    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        
        super.doFilter(request, response, chain);
    }


    private String getUrl(ServletRequest request) {
        String path = "";
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        path = httpRequest.getRequestURI().substring(httpRequest.getContextPath().length() + 1);
        return path;
    }


    private boolean matchPattern(String requestedPath, String includeUrl) {
        includeUrl = includeUrl.substring(1);
        String[] path = requestedPath.split("/");
        if (path.length > 1) {
            String[] roleFunctionArray = includeUrl.split("/");
            boolean match = true;
            for (int i = 0; i < roleFunctionArray.length; i++) {
                if (match) {
                    if (!roleFunctionArray[i].equals("*")) {
                        Pattern p = Pattern.compile(Pattern.quote(path[i]), Pattern.CASE_INSENSITIVE);
                        Matcher m = p.matcher(roleFunctionArray[i]);
                        match = m.matches();
                    } else if (roleFunctionArray[i].equals("*")) {
                        match = true;
                    }

                }
            }
            if (match)
                return match;
        } else {
            if (requestedPath.matches(includeUrl))
                return true;
            else if (includeUrl.equals("*"))
                return true;
        }
        return false;
    }
    

    public static List<AAFPermission> getAAFPermissions(HttpServletRequest request) { 
        CadiWrap wrapReq = (CadiWrap) request; 
        List<Permission> perms = wrapReq.getPermissions(wrapReq.getUserPrincipal()); 
        List<AAFPermission> aafPermsList = new ArrayList<>(); 
        for (Permission perm : perms) { 
            AAFPermission aafPerm = (AAFPermission) perm; 
            aafPermsList.add(aafPerm); 
            System.out.println(aafPerm.toString());
            System.out.println(aafPerm.getType());
        } 
        return aafPermsList; 
    } 
    
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
     * 
     * @param request 
     * @return returns list of AAFPermission for the specific namespace 
     */ 
    public static List<AAFPermission> getNameSpacesAAFPermissions(String nameSpace, 
            List<AAFPermission> allPermissionsList) { 
        String type = nameSpace + ".url"; 
        allPermissionsList.removeIf(perm -> (!perm.getType().equals(type))); 
        return allPermissionsList; 
    }
}