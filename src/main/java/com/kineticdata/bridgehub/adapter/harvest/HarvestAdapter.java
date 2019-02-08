package com.kineticdata.bridgehub.adapter.harvest;

import com.kineticdata.bridgehub.adapter.BridgeAdapter;
import com.kineticdata.bridgehub.adapter.BridgeError;
import com.kineticdata.bridgehub.adapter.BridgeRequest;
import com.kineticdata.bridgehub.adapter.Count;
import com.kineticdata.bridgehub.adapter.Record;
import com.kineticdata.bridgehub.adapter.RecordList;
import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.io.IOException;
import java.net.URLEncoder;
import org.apache.http.HttpEntity;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.LoggerFactory;

public class HarvestAdapter implements BridgeAdapter {
    /*----------------------------------------------------------------------------------------------
     * PROPERTIES
     *--------------------------------------------------------------------------------------------*/
    
    /** Defines the adapter display name */
    public static final String NAME = "Harvest Application Bridge";
    
    /** Defines the logger */
    protected static final org.slf4j.Logger logger = LoggerFactory.getLogger(HarvestAdapter.class);
    
    /** Defines the collection of property names for the adapter */
    public static class Properties {
        public static final String PROPERTY_ACCESS_TOKEN = "Access Token";
        public static final String PROPERTY_ACCOUNT_ID = "Account Id";
    }
    
    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
        new ConfigurableProperty(Properties.PROPERTY_ACCESS_TOKEN).setIsRequired(true),
        new ConfigurableProperty(Properties.PROPERTY_ACCOUNT_ID)
    );

    // Local variables to store the property values in
    private String accessToken;
    private String accountId;
    private String harvestEndpoint;
    
    /*---------------------------------------------------------------------------------------------
     * SETUP METHODS
     *-------------------------------------------------------------------------------------------*/
    
    @Override
    public void initialize() throws BridgeError {
        // Initializing the variables with the property values that were passed
        // when creating the bridge so that they are easier to use
        this.accessToken = properties.getValue(Properties.PROPERTY_ACCESS_TOKEN);
        this.accountId = properties.getValue(Properties.PROPERTY_ACCOUNT_ID);
        this.harvestEndpoint = "https://api.harvestapp.com/v2";
    }

    @Override
    public String getName() {
        return NAME;
    }
    
    @Override
    public String getVersion() {
        // Bridgehub uses this version instead of the Maven version when 
        // displaying it in the console
        return "1.0.0";
    }
    
    @Override
    public void setProperties(Map<String,String> parameters) {
        // This should always be the same unless there are special circumstances
        // for changing it
        properties.setValues(parameters);
    }
    
    @Override
    public ConfigurablePropertyMap getProperties() {
        // This should always be the same unless there are special circumstances
        // for changing it
        return properties;
    }
    
    // Structures that are valid to use in the bridge. Used to check against
    // when a method is called to make sure that the Structure the user is
    // attempting to call is valid
    public static final List<String> VALID_STRUCTURES = 
        Arrays.asList(new String[] {
        
        "Clients","Projects","Tasks", "Task Assignments", "Users",
        "User Assignments", "Time Entries"
    });
    
    /*---------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *-------------------------------------------------------------------------------------------*/

    @Override
    public Count count(BridgeRequest request) throws BridgeError {
        // Log the access
        logger.trace("Counting records");
        logger.trace("  Structure: " + request.getStructure());
        logger.trace("  Query: " + request.getQuery());
        
        // Check if the inputted structure is valid
        if (!VALID_STRUCTURES.contains(request.getStructure())) {
            throw new BridgeError("Invalid Structure: '"
                + request.getStructure() + "' is not a valid structure");
        }
        
        // Parse the query and exchange out any parameters with their parameter values
        HarvestQualificationParser parser = new HarvestQualificationParser();
        String query = parser.parse(request.getQuery(),request.getParameters());
       
        // Retrieve the objects based on the structure from the source
        String output = getResource(buildSearchUrl(request.getStructure(),query));
        logger.trace("Count Output: "+output);

        // Parse the response string into a JSONObject
        JSONObject object = (JSONObject)JSONValue.parse(output);
        
        // Get the number of elements in the returned array
        Long tempCount = (Long)object.get("total_entries");
        Integer count = (int)tempCount.intValue();
        
        // Create and return a count object that contains the count
        return new Count(count);
    }

    @Override
    public Record retrieve(BridgeRequest request) throws BridgeError {
        // Log the access
        logger.trace("Retrieving Kinetic Request CE Record");
        logger.trace("  Structure: " + request.getStructure());
        logger.trace("  Query: " + request.getQuery());
        logger.trace("  Fields: " + request.getFieldString());
        
        // Check if the inputted structure is valid
        if (!VALID_STRUCTURES.contains(request.getStructure())) {
            throw new BridgeError("Invalid Structure: '"
                + request.getStructure() + "' is not a valid structure");
        }
        
        // Parse the query and exchange out any parameters with their parameter values
        HarvestQualificationParser parser = new HarvestQualificationParser();
        String query = parser.parse(request.getQuery(),request.getParameters());

        // Retrieve the objects based on the structure from the source
        String output = getResource(buildRetrieveUrl(request.getStructure(), 
            query));
        logger.trace("Retrieve Output: "+output);
        
        // Parse the response string into a JSONObject
        JSONObject obj = (JSONObject)JSONValue.parse(output);
        
        List<String> fields = request.getFields();
        if (fields == null) { 
            fields = new ArrayList();
        }
        
        if(fields.isEmpty()){
            fields.addAll(obj.keySet());
        }
        
        // If specific fields were specified then we remove all of the 
        // nonspecified properties from the object.
        Set<Object> removeKeySet = new HashSet<Object>();
        for(Object key: obj.keySet()){
            if(fields.contains(key)){
                continue;
            }else{
                logger.trace("Remove Key: "+key);
                removeKeySet.add(key);
            }
        }
        obj.keySet().removeAll(removeKeySet);
        
        Object[] keys = obj.keySet().toArray();
        JSONObject convertedObj = convertValues(obj,keys);
        
        // Create a Record object from the responce JSONObject
        Record record;
        if (convertedObj != null) {
            record = new Record(convertedObj);
        } else {
            record = new Record();
        }
        
        // Return the created Record object
        return record;
    }

    @Override
    public RecordList search(BridgeRequest request) throws BridgeError {
        // Log the access
        logger.trace("Searching Records");
        logger.trace("  Structure: " + request.getStructure());
        if (request.getQuery() != null) {
            logger.trace("  Query: " + request.getQuery());
        }
        if (request.getFieldString() != null) {
            logger.trace("  Fields: " + request.getFieldString());
        }
        
        // Check if the inputted structure is valid
        if (!VALID_STRUCTURES.contains(request.getStructure())) {
            throw new BridgeError("Invalid Structure: '"
                + request.getStructure() + "' is not a valid structure");
        }
        
        // Parse the query and exchange out any parameters with their parameter values
        HarvestQualificationParser parser = new HarvestQualificationParser();
        String query = parser.parse(request.getQuery(),request.getParameters());
        
        // Retrieve the objects based on the structure from the source
        String output;
        if (request.getMetadata() != null) {
            output = getResource(buildSearchUrl(request.getStructure(), query,
                request.getMetadata()));
        } else {
            output = getResource(buildSearchUrl(request.getStructure(),query));
        }
        
        logger.trace("Search Output: " + output);
        
        // Parse the response string into a JSONObject
        JSONObject obj = (JSONObject)JSONValue.parse(output);

        // Get the array of objects. Each Structure has a different accessor name.
        JSONArray objects = (JSONArray)obj.get(
            request.getStructure().replaceAll(" ", "_").toLowerCase()
        );
        
        // Create a List of records that will be used to make a RecordList object.
        List<Record> recordList = new ArrayList<Record>();
        
        // If the user doesn't enter any values for fields we return all of the fields.
        List<String> fields = request.getFields();
        if (fields == null) { 
            fields = new ArrayList();
        }        

        if(objects.isEmpty() != true){
            JSONObject firstObj = (JSONObject)objects.get(0);

            Object[] keys = firstObj.keySet().toArray();
            
            Set<Object> removeKeySet = new HashSet<Object>();
            
            // If no keys where provided to the search then we return all
            // properties.
            if(fields.isEmpty()){
                fields.addAll(firstObj.keySet());
            } else {
            
                // If specific fields were specified then we remove all of the 
                // nonspecified properties from the object.
                for(Object key: firstObj.keySet()){
                    if(fields.contains(key)){
                        continue;
                    } else {
                        logger.trace("Remove Key: "+key);
                        removeKeySet.add(key);
                    }
                }
            }
            
            // Iterate through the responce objects and make a new Record for each.
            for (Object o : objects) {
                JSONObject object = (JSONObject)o;

                // Remove all keys that are not in the fields list.
                object.keySet().removeAll(removeKeySet);
                
                // Reset keys to the new object's keySet.
                keys = object.keySet().toArray();
                
                JSONObject convertedObj = convertValues(object,keys);
                Record record;
                if (convertedObj != null) {
                    record = new Record(convertedObj);
                } else {
                    record = new Record();
                }
                // Add the created record to the list of records
                recordList.add(record);
            }
        }
        
        // Return the RecordList object
        return new RecordList(fields, recordList);
    }
    
    /*--------------------------------------------------------------------------
     * HELPER METHODS
     *------------------------------------------------------------------------*/

    // Escape query helper method that is used to escape queries that have spaces
    // and other characters that need escaping to form a complete URL
    private String escapeQuery(Map<String,String> queryMap) {
        Object[] keyList =  queryMap.keySet().toArray();
        String queryStr = "";
        for (Object key: keyList) {
            String keyStr = key.toString().trim().replaceAll(" ","+");
            String value = queryMap.get(key);
            queryStr += keyStr+"="+URLEncoder.encode(value)+"&";
        }
        logger.trace("Query String: "+queryStr);
        return queryStr;
    }
    
    // Build a map of queries from the request.  Some of the queries will be 
    // used to build the url and some will get passed on to harvest.
    private Map<String,String> getQueryMap(String query) throws BridgeError {
        Map<String,String> queryMap = new HashMap<String,String>();
        String[] qSplit = query.split("&");
        for (int i=0;i<qSplit.length;i++) {
            String qPart = qSplit[i];
            String[] keyValuePair = qPart.split("=");
            String key = keyValuePair[0].trim();
            
            if (queryMap.containsKey(key)) {
                throw new BridgeError("A query can only contain one " + key 
                    + " parameter.");
            }
            String value = keyValuePair.length > 1 ? keyValuePair[1].trim() : "";
            logger.trace("Query Map Key: "+key+" Value: "+value);
            queryMap.put(key,value);
        }
        return queryMap;
    }
    
    // Each Structure requires it's own specific url and some require that a
    // query parameter be passed in.
    private String buildSearchUrl(String structure, String query) throws 
        BridgeError {
        
        return buildSearchUrl(structure, query, null);
    }
    
    // Overloaded method to handled metadata being passed in that must be added
    // to the url's query
    private String buildSearchUrl(String structure, String query, 
        Map<String,String> metadata) throws BridgeError {
        
        Map<String,String> queryMap = getQueryMap(query);
        
        String url = this.harvestEndpoint;
        if (structure.equals("Clients")){
            url += "/clients";
        } else if (structure.equals("Projects")){
            url += "/projects";
        } else if (structure.equals("Tasks")){
            url += "/tasks";
        } else if (structure.equals("Task Assignments")){
            if (queryMap.containsKey("project_id")){
                url += "/projects/" + queryMap.get("project_id") 
                    + "/task_assignments";
                queryMap.remove("project_id");
            } else {
                url += "/task_assignments";
            }
        } else if (structure.equals("Time Entries")){
            url += "/time_entries";
        } else if (structure.equals("Users")){
            url += "/users";
        } else if (structure.equals("Users Assignments")) {
            if (queryMap.containsKey("project_id")){
                url += "/projects/" + queryMap.get("project_id")
                    + "/user_assignments";
                queryMap.remove("project_id");
            } else {
                url += "/user_assignments";
            }
        } else { // This option is for the Project Assignments Structure
            if (queryMap.containsKey("user_id")){
                url += "/users/" + queryMap.get("user_id")
                    + "/project_assignments";
                queryMap.remove("project_id");
            } else {
                throw new BridgeError("A user_id is required for the Project"
                    + " Assignments structure");
            }
        }
        
        // Add the page value to the query map for pagination.        
        if (metadata != null && metadata.containsKey("page")) {
            queryMap.put("page", metadata.get("page"));
        }
        
        if (!queryMap.isEmpty() && !queryMap.containsKey("")) {
            // Harvest v2 api only allows a limit of 100 records per query
            if (queryMap.containsKey("per_page")
                && Integer.parseInt(queryMap.get("per_page")) > 100) {
                
                queryMap.put("per_page", "100");
            }
            url += "?" + escapeQuery(queryMap);
        }
        logger.trace("Search url: "+url);
        return url;
    }
    
    // Each Structure requires it's own specific url and some require that a 
    // query parameter be passed in. 
    private String buildRetrieveUrl(String structure, String query) throws 
        BridgeError{
        
        Map<String,String> queryMap = getQueryMap(query);
        String url = this.harvestEndpoint;
        if (structure.equals("Clients")) {
            if (queryMap.containsKey("client_id")) {
                url += "/clients/" + queryMap.get("client_id");
                queryMap.remove("client_id");
            } else {
                throw new BridgeError("A client_id is required to retrieve a"
                    + " client");
            }
        } else if(structure.equals("Projects")) {
            if (queryMap.containsKey("project_id")) {
                url += "/projects/" + queryMap.get("project_id");
                queryMap.remove("project_id");
            } else {
                throw new BridgeError("A project_id is required to retrieve a"
                    + " project");
            }
        } else if(structure.equals("Tasks")) {
            url += "/tasks";
            if (queryMap.containsKey("task_id")) {
                url += "/tasks/" + queryMap.get("task_id");
                queryMap.remove("task_id");
            } else {
                throw new BridgeError("A task_id is required to retrieve a task");
            }
        } else if(structure.equals("Task Assignments")) {
            if (queryMap.containsKey("project_id")
                && queryMap.containsKey("task_assignment_id")) {
                
                url += "/projects/"+queryMap.get("project_id")
                    + "/task_assignments/" + queryMap.get("task_assignment_id");
                queryMap.remove("project_id");
                queryMap.remove("task_assignment_id");
            } else {
                throw new BridgeError("A project_id and task_assignment_id are"
                    + " required to retrieve a task assignment");
            }
        } else if (structure.equals("Users")) {
            if (queryMap.containsKey("user_id")) {
                url += "/users/" + queryMap.get("user_id");
                queryMap.remove("user_id");
            } else {
                throw new BridgeError("A user_id is required to retrieve a user");
            }
        } else if (structure.equals("Users Assignment")) {
            if (queryMap.containsKey("project_id")
                && queryMap.containsKey("user_assignment_id")) {
                
                url += "/projects/" + queryMap.get("project_id") 
                    + "/user_assignments" + queryMap.get("user_assignment_id");
                queryMap.remove("project_id");
                queryMap.remove("user_assignment_id");
            } else {
                throw new BridgeError("A project_id and user_assignment_id are"
                    + " required to retrieve users assignment");
            }
        } else {
            throw new BridgeError("The " + structure + " structure does not"
                + " have a retrieve option");
        }
        logger.trace("Retrieve url: "+url);
        return url;
    }
    
    // Count Search and Retrieve get the resoucre the same and use the same
    // output object.
    private String getResource(String url) throws BridgeError {
        // Initialize the HTTP Client,Response, and HTTP GET objects
        HttpClient client = new DefaultHttpClient();
        HttpResponse response;
        HttpGet get = new HttpGet(url);

        // Append HTTP BASIC Authorization header to HttpGet call
        String accessToken = this.accessToken;
        String accountId = this.accountId;
        get.setHeader("Authorization", "Bearer " + accessToken);
        get.setHeader("Harvest-Account-ID", accountId);
        get.setHeader("User-Agent", "Kinetic Data, Inc");
        get.setHeader("Content-Type", "application/json");
        get.setHeader("Accept", "application/json");
        
        // Make the call to the source to retrieve data and convert the response
        // from a HttpEntity object into a Java String
        String output = "";
        try {
            response = client.execute(get);
            HttpEntity entity = response.getEntity();
            output = EntityUtils.toString(entity);
            int responseCode = response.getStatusLine().getStatusCode();
            logger.trace("Request response code: " + responseCode);
            if(responseCode == 404){
                throw new BridgeError("404 Page not found at "+url+".");
            }else if(responseCode == 401){
                throw new BridgeError("401 Access on valid.");
            }
        } 
        catch (IOException e) {
            logger.error(e.getMessage());
            throw new BridgeError("Unable to make a connection to Kinetic Core.", e);
        }
        return output;
    }
 
    // This method converts non string values to strings.
    private JSONObject convertValues(JSONObject obj,Object[] keys){
        for (Object key : keys){
            Object value = obj.get((String)key);
            if (!(value instanceof String)) {
                logger.trace("Converting: " + String.valueOf(value) + " to a string");
                obj.put((String)key, String.valueOf(value));
            }
        } 
        return obj;
    }
}
