package org.onap.music.unittests.authentication;

import static org.junit.Assert.*;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.ServletRequest;
import org.junit.Test;
import org.mockito.Mockito;
import org.onap.aaf.cadi.CadiWrap;
import org.onap.aaf.cadi.Permission;
import org.onap.aaf.cadi.aaf.AAFPermission;
import org.onap.music.authentication.AuthUtil;

public class AuthUtilTest {

    @Test
    public void testGetAAFPermissions() {
        CadiWrap cw = Mockito.mock(CadiWrap.class);
        List<Permission> permList = new ArrayList<Permission>();
        Permission perm1 = Mockito.mock(AAFPermission.class);
        permList.add(perm1);
        Mockito.when(cw.getPermissions(Mockito.any())).thenReturn(permList);

        List<AAFPermission> returnedPerm = AuthUtil.getAAFPermissions(cw);
        
        assertEquals(perm1, returnedPerm.get(0));
    }

    @Test
    public void testDecodeFunctionCode() throws Exception {
        String toDecode = "some%2dthing.something.%2a";
        String decoded = AuthUtil.decodeFunctionCode(toDecode);
        
        assertEquals("some-thing.something.*", decoded);
    }

    @Test
    public void testIsAccessAllowed() throws Exception {
        System.out.println("Request perms");
        assertTrue(AuthUtil.isAccessAllowed(createRequest("*", "*"), "testns"));
    }
    
    @Test
    public void testIsAccessNotAllowed() throws Exception {
        System.out.println("Request to write when have read perms");
        assertFalse(AuthUtil.isAccessAllowed(createRequest("POST", "GET"), "testns"));
    }
    
    @Test
    public void testIsAccessAllowedNullRequest() {
        try {
            assertFalse(AuthUtil.isAccessAllowed(null, "namespace"));
            fail("Should throw exception");
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testIsAccessAllowedNullNamespace() {
        try {
            assertFalse(AuthUtil.isAccessAllowed(createRequest(), null));
            fail("Should throw exception");
        } catch (Exception e) {
        }
    }
    
    @Test
    public void testIsAccessAllowedEmptyNamespace() {
        try {
            assertFalse(AuthUtil.isAccessAllowed(createRequest(), ""));
            fail("Should throw exception");
        } catch (Exception e) {
        }
    }

    /**
     * 
     * @param permRequested 'PUT', 'POST', 'GET', or 'DELETE'
     * @param permGranted '*' or 'GET'
     * @return
     */
    private ServletRequest createRequest(String permRequested, String permGranted) {
        CadiWrap cw = Mockito.mock(CadiWrap.class);
        List<Permission> permList = new ArrayList<Permission>();
        AAFPermission perm1 = Mockito.mock(AAFPermission.class);
        Mockito.when(perm1.getType()).thenReturn("testns");
        Mockito.when(perm1.getKey()).thenReturn("org.onap.music.api.user.access|testns|" + permGranted);

        permList.add(perm1);
        Mockito.when(cw.getPermissions(Mockito.any())).thenReturn(permList);
        Mockito.when(cw.getRequestURI()).thenReturn("/v2/locks/create/testns.MyTable.Field1");
        Mockito.when(cw.getContextPath()).thenReturn("/v2/locks/create");
        Mockito.when(cw.getMethod()).thenReturn(permRequested);
        
        return cw;
    }
    
    private ServletRequest createRequest() {
        return createRequest("POST","*");
    }
}
