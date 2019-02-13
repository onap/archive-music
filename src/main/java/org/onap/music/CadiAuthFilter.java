/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 *  Copyright (c) 2017 AT&T Intellectual Property
 *
 *  Modifications Copyright (C) 2019 IBM.
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
import java.util.ArrayList;
import java.util.List;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.http.HttpServletRequest;

import org.onap.aaf.cadi.CadiWrap;
import org.onap.aaf.cadi.Permission;
import org.onap.aaf.cadi.PropAccess;
import org.onap.aaf.cadi.aaf.AAFPermission;
import org.onap.aaf.cadi.filter.CadiFilter;

public class CadiAuthFilter extends CadiFilter {

    public CadiAuthFilter(boolean init, PropAccess access) throws ServletException {
        super(true, access);
    }

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        super.init(filterConfig);
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