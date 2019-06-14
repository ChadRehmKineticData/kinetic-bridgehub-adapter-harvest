import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeAdapterTestBase;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.bridgehub.adapter.harvest.HarvestAdapter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class HarvestTest extends BridgeAdapterTestBase{
        
    @Override
    public Class getAdapterClass() {
        return HarvestAdapter.class;
    }
    
    @Override
    public String getConfigFilePath() {
        return "src/test/resources/bridge-config.yml";
    }
    
    @Test
    public void test_count() throws Exception{
        BridgeError error = null;

        BridgeRequest request = new BridgeRequest();
        
        List<String> fields = Arrays.asList("id");
        request.setFields(fields);
        
        request.setStructure("Projects");
        request.setFields(fields);
        request.setQuery("projects");
        
        Count count = null;
        try {
            count = getAdapter().count(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(count.getValue() > 0);
    }
    
    @Test
    public void test_count_param() throws Exception{
        BridgeError error = null;

        BridgeRequest request = new BridgeRequest();
        
        List<String> fields = Arrays.asList("id");
        request.setFields(fields);
        
        request.setStructure("Projects");
        request.setFields(fields);
        request.setQuery("projects?is_active=<%=parameter[\"Is Active\"]%>");
        
        Map parameters = new HashMap();
        parameters.put("Is Active", "true");
        request.setParameters(parameters);
        
        Count count = null;
        try {
            count = getAdapter().count(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(count.getValue() > 0);
    }
    
    @Test
    public void test_search() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        fields.add("client");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Projects");
        request.setFields(fields);
        request.setQuery("projects?is_active=<%=parameter[\"Is Active\"]%>"
                       + "&client_id=<%=parameter[\"Client Id\"]%>");
                
        Map parameters = new HashMap();
        parameters.put("Is Active", "true");
        parameters.put("Client Id", "2319519");
        request.setParameters(parameters);
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }
    
        @Test
    public void test_search_empty_fields() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Projects");
        request.setQuery("projects?is_active=<%=parameter[\"Is Active\"]%>"
                       + "&client_id=<%=parameter[\"Client Id\"]%>");
                
        Map parameters = new HashMap();
        parameters.put("Is Active", "true");
        parameters.put("Client Id", "2319519");
        request.setParameters(parameters);
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }
    
        
    @Test
    public void test_user_dec_sort() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        fields.add("first_name");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Users");
        request.setFields(fields);
        request.setQuery("users?per_page=<%=parameter[\"Per Page\"]%>"
                       + "&is_active=<%=parameter[\"Is Active\"]%>");
        
        Map parameters = new HashMap();
        parameters.put("Is Active", "false");
        parameters.put("Per Page", "75");
        request.setParameters(parameters);
        
        Map<String,String> metadata = new HashMap();
        
        metadata.put("page", "1");
        metadata.put("order", "DESC");
        request.setMetadata(metadata);
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }
    
    @Test
    public void test_user_search_param() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        fields.add("first_name");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Users");
        request.setFields(fields);
        request.setQuery("users?per_page=<%=parameter[\"Per Page\"]%>"
                       + "&is_active=<%=parameter[\"Is Active\"]%>");
        
        Map parameters = new HashMap();
        parameters.put("Is Active", "false");
        parameters.put("Per Page", "75");
        request.setParameters(parameters);
        
        Map<String,String> metadata = new HashMap();
        
        metadata.put("page", "1");
        
        request.setMetadata(metadata);
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }
    
    @Test
    public void test_clients_search() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Clients");
        request.setFields(fields);
        request.setQuery("clients");
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }
     
    @Test
    public void test_clients_metadata() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Clients");
        request.setFields(fields);
        request.setQuery("clients");
        
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("order", "<%=field[\"client\"]%>:ASC,<%=field[\"id\"]%>:ASC,<%=field[\"project\"]%>:DESC");
        metadata.put("page","2");
        request.setMetadata(metadata);
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }
    
    @Test
    public void test_metadata_last_page() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Clients");
        request.setFields(fields);
        request.setQuery("clients");
        
        Map<String, String> metadata = new HashMap<String, String>();
        metadata.put("page","3");
        request.setMetadata(metadata);
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }
    
    @Test
    public void test_tasks_search() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Tasks");
        request.setFields(fields);
        request.setQuery("tasks");
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }
 
    @Test
    public void test_task_assignments_search() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Task Assignments");
        request.setFields(fields);
        request.setQuery("task_assignments");
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }

    @Test
    public void test_user_assignments_search() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("User Assignments");
        request.setFields(fields);
        request.setQuery("user_assignments");
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }

    @Test
    public void test_time_entries_search() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Time Entries");
        request.setFields(fields);
        request.setQuery("time_entries");
        
        RecordList list = null;
        try {
            list = getAdapter().search(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(list.getRecords().size() > 0);
    }
     
    @Test
    public void test_retrieve() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Projects");
        request.setFields(fields);
        request.setQuery("projects/<%=parameter[\"Project Id\"]%>");
        
        Map parameters = new HashMap();
        parameters.put("Project Id", "11016819");
        request.setParameters(parameters);
        
        Record record = null;
        try {
            record = getAdapter().retrieve(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(record.getRecord().containsKey("id"));
    }
    
    @Test
    public void test_user_retrieve() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        fields.add("first_name");
        fields.add("last_name");
        fields.add("roles");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Users");
        request.setFields(fields);
        request.setQuery("users/<%=parameter[\"User Id\"]%>");
        
        Map parameters = new HashMap();
        parameters.put("User Id", "1075388");
        request.setParameters(parameters);
        
        Record record = null;
        try {
            record = getAdapter().retrieve(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(record.getRecord().containsKey("id"));
    }
    
    @Test
    public void test_build_query() throws Exception{
        
        HarvestAdapter adapter = new HarvestAdapter();
          
        Map<String,String> metadata = new HashMap();
        metadata.put("page", "1");
        
        List<NameValuePair> parameters = new ArrayList<NameValuePair>();
        parameters.add(new BasicNameValuePair("page", "2"));
        parameters.add(new BasicNameValuePair("is_active", "true"));
          
        String encodedUrl = adapter.buildQuery(parameters, metadata);
        
        assertEquals("is_active=true&page=2", encodedUrl);
    }
}
