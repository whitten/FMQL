import java.net.*;
import java.io.*;
import java.util.*;
import com.google.gson.*; /* http://code.google.com/p/google-gson/ */

/* 
 * FMQL - Examples of querying from Java
 * - uses .net and google-gson (no thread pools etc)
 * - just walks json and dumps as a string: this is dynamic coding
 *
 * TBD: 
 * - unlike Javascript or Python coding, Java emphasizes types.
 * Add in a type for basic reply JSON ie use fromJSON
 * - distinguish typed-literal from literal (see date below)
 * - walk cnode arrays too ie recurse
 *
 * To use:
 * - download gson JAR and make sure it is in your classpath
 *   ... https://code.google.com/p/google-gson/downloads/list
 * - crude invocation if JAR for gson is in FMQLReport's directory
 *   - javac -classpath 'gson-1.6.jar' FMQLReports.java
 *   - java -classpath '.;gson-1.6.jar' FMQLReports
 *
 * LICENSE:
 * This program is free software; you can redistribute it and/or modify it under the terms of
 * the GNU Affero General Public License version 3 (AGPL) as published by the Free Software 
 * Foundation.
 * (c) 2014 caregraf
 */
public class FMQLReports {

    /* Change to your own URL once you've set up FMQL on your VistA */
    /* public static String FMQLEP = "http://livevista.caregraf.info/fmqlEP"; */
    public static String FMQLEP = "http://www.examplehospital.com/fmqlEP";
    
    /* A sample of FMQL's data queries. Form is Name, basic query, filter if any */
    /* TIP: to know a file's id or its field ids or ... look at the File's schema in the Rambler */
    public static String[][] QUERIES = { 
        {"Data: list first 100 patients (2)", "SELECT 2 LIMIT 100"},
        /* Using patient 9 as that is fullish in Caregraf's demo VistA. Pick an equivalent in yours */
        {"Data: describe patient 9", "DESCRIBE 2-9"},
        {"Data: list all vitals (120_5) of patient 9", "SELECT 120_5 FILTER(.02=2-9)"},
        {"Data: describe all vitals of patient 9, not entered in error", "DESCRIBE 120_5 FILTER(.02=2-9&!bound(2))"},
        {"Data: describe all height measurements (120_5) of patient 9, not entered in error, since January 2008", "DESCRIBE 120_5 FILTER(.02=2-9&!bound(2)&.03=120_51-8&.01>2008-01-01)"},
    };
    
    public void request(String queryDescr, String query) throws Exception {
    
        System.out.println("====== " + queryDescr + " ======");
        String fmqlrs = FMQLEP + "?fmql=" + URLEncoder.encode(query, "UTF-8");
        URL fmqlr = new URL(fmqlrs);
        // 1. Make the query
        URLConnection fmqlc = fmqlr.openConnection();
        // 2. Read the Response
        BufferedReader in = new BufferedReader(
                                new InputStreamReader(
                                fmqlc.getInputStream()));
        // 3. Parse as JSON
        JsonParser parser=new JsonParser();
        // 4. Walk the JSON
        JsonObject reply=parser.parse(in).getAsJsonObject();
        if (reply.has("results"))
        {
            // query reply has form {"results": [...]}
            JsonArray results = reply.getAsJsonArray("results");
            System.out.println("There were " + results.size() + " results");
            for (Iterator<JsonElement> it = results.iterator(); it.hasNext(); )
            {
                System.out.println("---------------------------------\n");

                // result has form {"predicate1": {structured value}, "predicate2": {...}}
                JsonObject result = it.next().getAsJsonObject();
                for (Iterator iterator = result.entrySet().iterator(); iterator.hasNext(); ) {
                    Map.Entry assertion = (Map.Entry) iterator.next();
                    String predicate = assertion.getKey().toString();
                    // structured value has form: 
                    // {"value": "...", "type": "uri | literal | typed-literal | cnodes" ...}
                    JsonObject valueObject = (JsonObject)((JsonElement)assertion.getValue()).getAsJsonObject();
                    // Interpret the value based on its type
                    String valueType = valueObject.get("type").getAsString();
                    if (valueType.equals("uri")) {
                        // uris (node references) come with labels
                        String uriLabel = valueObject.get("label").getAsString();
                        String uriValue = valueObject.get("value").getAsString();
                        System.out.println("\t" + predicate);
                        System.out.println("\t\t" + uriLabel + " (" + uriValue + ")");                  
                    }
                    // Lazy: treating typed-literals (like text from word processing or date) like simple strings
                    else if ((valueType.equals("literal"))||(valueType.equals("typed-literal")))
                    {
                        System.out.println("\t" + predicate);
                        System.out.println("\t\t" + valueObject.get("value").getAsString());
                    }
                    // For now, ignoring CNodes.
                }
            }
        }

        System.out.println("===============================\n\n");
    }

    public static void main(String[] args) throws Exception {
        FMQLReports fmqlReports = new FMQLReports();
        for (int i=0; i<QUERIES.length; i++) {
            fmqlReports.request(QUERIES[i][0], QUERIES[i][1]);
        }
    }
}
