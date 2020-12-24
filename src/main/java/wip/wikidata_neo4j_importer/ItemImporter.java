package wip.wikidata_neo4j_importer;

import org.json.JSONArray;
import org.json.JSONObject;
import org.neo4j.batchinsert.BatchInserter;
import org.neo4j.batchinsert.BatchInserters;
import org.neo4j.graphdb.Label;
import org.neo4j.io.layout.DatabaseLayout;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;

public class ItemImporter {
    Map<String, Function<JSONObject, Object>> datatypeMapper;
    Set<String> dataTypeBlacklist;
    Map<String, String> propId2Name;

    Label labelItem; // group nodes of items
    Label labelProp; // group nodes of properties

    // Write all properties to a file.
    // We don't use them during importing actually.
    // But it can be convenient and efficient to use this dump,
    // rather than original json file if you want to add additional information to edges.
    PrintWriter propertyWriter;

    BatchInserter inserter;

    public ItemImporter(String pathNeo4jDatabase, String propDumpPath, String propId2NamePath) throws IOException {
        labelItem = Label.label("Item");
        labelProp = Label.label("Property");
        propertyWriter = new PrintWriter(propDumpPath);

        initializeInserter(pathNeo4jDatabase);
        initializePropertyDependency(propId2NamePath);
    }

    private void initializePropertyDependency(String propId2NamePath) {
        try {
            propId2Name = readPropId2Name(propId2NamePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        dataTypeBlacklist = new HashSet<>();
        dataTypeBlacklist.add("commonsMedia");
        dataTypeBlacklist.add("external-id");
        dataTypeBlacklist.add("geo-shape");
        dataTypeBlacklist.add("wikibase-lexeme");
        dataTypeBlacklist.add("wikibase-property");
        dataTypeBlacklist.add("tabular-data");
        dataTypeBlacklist.add("wikibase-form");
        dataTypeBlacklist.add("wikibase-item");
        datatypeMapper = new HashMap<>();
        datatypeMapper.put("monolingualtext", ItemImporter::onlyKeepEnglish);
        datatypeMapper.put("quantity", ItemImporter::quantityToDouble);
        datatypeMapper.put("globe-coordinate", ItemImporter::coordinateToString);
        datatypeMapper.put("time", ItemImporter::timeToString);
        datatypeMapper.put("url", ItemImporter::valueToString);
        datatypeMapper.put("string", ItemImporter::valueToString);
        datatypeMapper.put("math", ItemImporter::valueToString);
    }

    private Map<String, String> readPropId2Name(String filepath) throws IOException {
        if (filepath == null) {
            return null;
        }
        BufferedReader inputReader = new BufferedReader(new FileReader(filepath));
        Map<String, String> ret = new HashMap<>();
        String line;
        while ((line = inputReader.readLine()) != null) {
            JSONObject obj = new JSONObject(line);
            String propId = obj.getString("wikidataId");
            String name = obj.getString("label");
            ret.put(propId, name);
        }
        return ret;
    }

    private static String onlyKeepEnglish(JSONObject obj) {
        JSONObject dataValue = obj.getJSONObject("mainsnak").getJSONObject("datavalue").getJSONObject("value");
        if ("en".equals(dataValue.getString("language"))) {
            return dataValue.getString("text");
        } else {
            return null;
        }
    }

    private static Double quantityToDouble(JSONObject obj) {
        JSONObject dataValue = obj.getJSONObject("mainsnak").getJSONObject("datavalue").getJSONObject("value");
        return Double.parseDouble(dataValue.getString("amount"));
    }

    private static String coordinateToString(JSONObject obj) {
        JSONObject dataValue = obj.getJSONObject("mainsnak").getJSONObject("datavalue").getJSONObject("value");
        String latitude = Double.toString(dataValue.getDouble("latitude"));
        String longitude = Double.toString(dataValue.getDouble("longitude"));
        return latitude + "," + longitude;
    }

    private static String timeToString(JSONObject obj) {
        JSONObject dataValue = obj.getJSONObject("mainsnak").getJSONObject("datavalue").getJSONObject("value");
        return dataValue.getString("time");
    }

    private static String valueToString(JSONObject obj) {
        return obj.getJSONObject("mainsnak").getJSONObject("datavalue").getString("value");
    }

    public void initializeInserter(String pathNeo4jDatabase) throws IOException {
        inserter = BatchInserters.inserter(DatabaseLayout.ofFlat(Paths.get(pathNeo4jDatabase)));
        // inserter.createDeferredSchemaIndex(labelItem).on("wikidataId").create();
    }

    public void importItem(String itemDocStr, Boolean isItem) {
        // Extract key information from json string
        JSONObject obj = new JSONObject(itemDocStr);
        String wikidataId = obj.getString("id");
        String datatype = getDatatype(obj);    // only exists in property
        String label = getEnLabel(obj);
        String description = getEnDescription(obj);
        String[] aliases = getEnAliases(obj);

        // Construct property map of current node
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("wikidataId", wikidataId);
        if (!datatype.equals("")) properties.put("datatype", datatype);
        if (!label.equals("")) properties.put("label", label);
        if (!description.equals("")) properties.put("description", description);
        if (aliases.length > 0) properties.put("aliases", aliases);
        extendClaimProperties(obj, properties);

        // Generate id of current node and insert it
        long nodeId = Long.parseLong(wikidataId.substring(1));
        if (isItem) {
            nodeId = Util.addPrefixToLong(nodeId, Config.itemPrefix, 10);
            if (!inserter.nodeExists(nodeId)) {
                inserter.createNode(nodeId, properties, labelItem);
            } else {
                inserter.setNodeProperties(nodeId, properties);
            }
        } else {
            nodeId = Util.addPrefixToLong(nodeId, Config.propPrefix, 10);
            if (!inserter.nodeExists(nodeId)) {
                inserter.createNode(nodeId, properties, labelProp);
            } else {
                inserter.setNodeProperties(nodeId, properties);
            }
        }

    // If current document is a property, dump it
        if (!isItem) {
            JSONObject resObj = new JSONObject();
            resObj.put("wikidataId", wikidataId);
            resObj.put("label", label);
            resObj.put("datatype", datatype);
            resObj.put("description", description);
            resObj.put("aliases", aliases);

            propertyWriter.write(resObj.toString() + "\n");
        }
    }

    public void shutDownNeo4j(){
        inserter.shutdown();
    }

    public void close() {
        inserter.shutdown();
        propertyWriter.close();
    }

    private String getDatatype(JSONObject obj) {
        if (!obj.has("datatype")) return "";
        return obj.getString("datatype");
    }

    private String getEnLabel(JSONObject obj) {
        if (!obj.has("labels")) return "";
        if (!obj.getJSONObject("labels").has("en")) return "";
        return obj.getJSONObject("labels").getJSONObject("en").getString("value");
    }

    private String getEnDescription(JSONObject obj) {
        if (!obj.has("descriptions")) return "";
        if (!obj.getJSONObject("descriptions").has("en")) return "";
        return obj.getJSONObject("descriptions").getJSONObject("en").getString("value");
    }

    private String[] getEnAliases(JSONObject obj) {
        if (!obj.has("aliases")) return new String[0];
        if (!obj.getJSONObject("aliases").has("en")) return new String[0];

        JSONArray aliases = obj.getJSONObject("aliases").getJSONArray("en");
        List<String> ret = new ArrayList<>();
        for (Object aliasObj : aliases) {
            String tempAlias = ((JSONObject) aliasObj).getString("value");
            ret.add(tempAlias);
        }
        return ret.toArray(new String[0]);
    }

    private String getPropNameById(String propId) {
        if (propId2Name == null) {
            return null;
        }
        return propId2Name.get(propId);
    }

    private void extendClaimProperties(JSONObject obj, Map<String, Object> properties) {
        if (propId2Name == null) {
            return;
        }
        JSONObject claimObj = obj.getJSONObject("claims");
        for (String propId : claimObj.keySet()) {
            JSONArray valueArray = claimObj.getJSONArray(propId);
            String snakType = valueArray.getJSONObject(0).getJSONObject("mainsnak").getString("snaktype");
            if (!snakType.equals("value")) continue;    // ignore some value and no value
            String dataType = valueArray.getJSONObject(0).getJSONObject("mainsnak").getString("datatype");
            if (dataTypeBlacklist.contains(dataType)) {
                continue;
            }
            Function<JSONObject, Object> valueMapFunc = datatypeMapper.getOrDefault(dataType, ItemImporter::valueToString);
            List<Object> values = new ArrayList<>();
            String propName = getPropNameById(propId);
            if (propName == null) {
                System.err.println("Skip... Unkown Property Name " + propId);
            }
            for (int i = 0; i < valueArray.length(); i++) {
                JSONObject value = valueArray.getJSONObject(i);
                if (value.keySet().contains("qualifiers")) {
                    continue;
                }
                Object mapped;
                try {
                    mapped = valueMapFunc.apply(value);
                } catch (Exception e) {
                    System.err.println("Hit Exception when parsing datatype " + dataType);
                    e.printStackTrace();
                    continue;
                }
                if (mapped != null) {
                    values.add(mapped);
                }
            }
            if (!values.isEmpty()) {
                Object[] valuesArr;
                if (values.get(0) instanceof String) {
                    valuesArr = values.toArray(new String[0]);
                } else if (values.get(0) instanceof Number) {
                    valuesArr = values.toArray(new Double[0]);
                } else {
                    System.err.println("Skip... Unkown Mapped Value Type " + values.get(0));
                    continue;
                }
                properties.put(propName, valuesArr);
            }
        }
    }

}
