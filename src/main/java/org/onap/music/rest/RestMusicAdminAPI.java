/*
 * ============LICENSE_START==========================================
 * org.onap.music
 * ===================================================================
 * Copyright (c) 2017 AT&T Intellectual Property
 * ===================================================================
 * Modifications Copyright (c) 2018-2019 IBM.
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
package org.onap.music.rest;

import javax.ws.rs.DELETE;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.core.Response;

import org.onap.music.datastore.jsonobjects.JSONObject;
import org.onap.music.datastore.jsonobjects.JsonOnboard;
import org.onap.music.eelf.logging.EELFLoggerDelegate;
import org.onap.music.rest.service.RestMusicAdminService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Controller
@RequestMapping("/v2/admin")
// @Path("/v{version: [0-9]+}/admin")
// @Path("/admin")
@Api(value = "Admin Api", hidden = true)
public class RestMusicAdminAPI {
    private static EELFLoggerDelegate logger = EELFLoggerDelegate.getLogger(RestMusicAdminAPI.class);
    @Autowired
    private RestMusicAdminService restAdminService;

    /*
     * API to onboard an application with MUSIC. This is the mandatory first
     * step.
     * 
     */
    @POST
    @RequestMapping("/onboardAppWithMusic")
    @ApiOperation(value = "Onboard application", response = String.class)
    public @ResponseBody Response onboardAppWithMusic(@RequestBody JsonOnboard jsonObj) throws Exception {
        Response res = restAdminService.onboardAppWithMusic(jsonObj);
        return res;
    }

    @POST
    @RequestMapping("/search")
    @ApiOperation(value = "Search Onboard application", response = String.class)
    public @ResponseBody Response getOnboardedInfoSearch(@RequestBody JsonOnboard jsonObj) throws Exception {
        Response res = restAdminService.getOnboardedInfoSearch(jsonObj);
        return res;
    }

    @DELETE
    @RequestMapping("/deleteOnboardApp")
    @ApiOperation(value = "Delete Onboard application", response = String.class)
    public @ResponseBody Response deleteOnboardApp(@RequestBody JsonOnboard jsonObj) throws Exception {
        Response res = restAdminService.deleteOnboardApp(jsonObj);
        return res;
    }

    @PUT
    @RequestMapping("/updateOnboardApp")
    @ApiOperation(value = "Update Onboard application", response = String.class)
    public @ResponseBody Response updateOnboardApp(@RequestBody JsonOnboard jsonObj) throws Exception {
        Response res = restAdminService.updateOnboardApp(jsonObj);
        return res;
    }

    @POST
    @RequestMapping("/callbackOps")
    public String callbackOps(JSONObject inputJsonObj) throws Exception {

        System.out.println("Input JSON: " + inputJsonObj.getData());
        return "Success";
    }
}
