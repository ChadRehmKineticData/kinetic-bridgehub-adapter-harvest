import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeAdapterTestBase;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.bridgehub.adapter.harvest.HarvestAdapter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    public void testCount() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Projects");
        request.setFields(fields);
        request.setQuery("");
        
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
    public void testSearch() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        fields.add("client");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Projects");
        request.setFields(fields);
        request.setQuery("is_active=true&client_id=2319519");
        
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
    public void testUserSearch() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        fields.add("first_name");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Users");
        request.setFields(fields);
        request.setQuery("per_page=75&is_active=false");
        
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
    public void testClientsSearch() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Clients");
        request.setFields(fields);
        request.setQuery("");
        
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
    public void testTasksSearch() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Tasks");
        request.setFields(fields);
        request.setQuery("");
        
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
    public void testTaskAssignmentsSearch() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Task Assignments");
        request.setFields(fields);
        request.setQuery("");
        
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
    public void testUserAssignmentsSearch() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("User Assignments");
        request.setFields(fields);
        request.setQuery("");
        
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
    public void testTimeEntriesSearch() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Time Entries");
        request.setFields(fields);
        request.setQuery("");
        
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
    public void testRetrieve() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Projects");
        request.setFields(fields);
        request.setQuery("project_id=11016819");
        
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
    public void testUserRetrieve() throws Exception{
        BridgeError error = null;
        
        assertNull(error);
        
        // Create the Bridge Request
        List<String> fields = new ArrayList<String>();
        fields.add("id");
        fields.add("first_name");
        fields.add("last_name");
        
        BridgeRequest request = new BridgeRequest();
        request.setStructure("Users");
        request.setFields(fields);
        request.setQuery("user_id=1075388");
        
        Record record = null;
        try {
            record = getAdapter().retrieve(request);
        } catch (BridgeError e) {
            error = e;
        }
        
        assertNull(error);
        assertTrue(record.getRecord().containsKey("id"));
    }
}
