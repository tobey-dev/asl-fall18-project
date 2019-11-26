package ch.ethz.asltest.Middleware.Global;

import ch.ethz.asltest.Middleware.Log.Log;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/*
    The Parameters class stores all parameters read from the MWParameters.xml config file located
    in resources.
 */

public class Parameters {
    private static Parameters ourInstance;
    private static boolean parsed = false;

    public static void initialize() {
         ourInstance = new Parameters();
    }

    public static Parameters getInstance() {
        return ourInstance;
    }

    private static HashMap<String, String> stringSettings;
    private static HashMap<String, Boolean> booleanSettings;
    private static HashMap<String, Integer> integerSettings;
    private static HashMap<String, Long> longSettings;

    private static final String stringTag = "string";
    private static final String boolTag = "bool";
    private static final String intTag = "int";
    private static final String longTag = "long";

    private static final String nameTag = "name";
    private static final String valueTag = "value";

    private static final String trueTag = "true";
    private static final String falseTag = "false";


    private Parameters() {

    }

    public static boolean isParsed(){
        return parsed;
    }

    public static void parse(){
        Log.info("[Parameters] Parsing parameters");
        String parameterFileProperty = System.getProperty("asl-fall18-project.configurationFile");
        if (parameterFileProperty != null){
            try {
                File parameterFile = new File(parameterFileProperty);
                readParameters(new FileInputStream(parameterFile));
            } catch (FileNotFoundException fileNotFoundException){
                Log.error("[Parameters] Property 'asl-fall18-project.configurationFile' set, but specified file '" + parameterFileProperty + "' could not be found");
            }
        } else {
            readParameters(Parameters.class.getResourceAsStream("/MWParameters.xml"));
        }
    }

    public static String getString(String name){
        if (!stringSettings.containsKey(name)){
            Log.error("[Parameters] String parameter '" + name + "' requested, but not available");
            return "";
        }
        return stringSettings.get(name);
    }

    public static Boolean getBoolean(String name){
        if (!booleanSettings.containsKey(name)){
            Log.error("[Parameters] Boolean parameter '" + name + "' requested, but not available");
            return false;
        }
        return booleanSettings.get(name);
    }

    public static Integer getInteger(String name){
        if (!integerSettings.containsKey(name)){
            Log.error("[Parameters] Integer parameter '" + name + "' requested, but not available");
            return 0;
        }
        return integerSettings.get(name);
    }

    public static Long getLong(String name){
        if (!longSettings.containsKey(name)){
            Log.error("[Parameters] Long parameter '" + name + "' requested, but not available");
            return 0L;
        }
        return longSettings.get(name);
    }

    private static void readParameters(InputStream inputStream){
        stringSettings = new HashMap<>();
        booleanSettings = new HashMap<>();
        integerSettings = new HashMap<>();
        longSettings = new HashMap<>();

        try {

            DocumentBuilderFactory documentBuilderFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            Document document = documentBuilder.parse(inputStream);

            document.getDocumentElement().normalize();
            if (!document.getDocumentElement().getNodeName().equals("parameters")){
                throw new Exception("Incompatible parameters file");
            }

            List<String> types = new ArrayList<>();
            types.add(stringTag);
            types.add(boolTag);
            types.add(intTag);
            types.add(longTag);


            for (String type : types){
                NodeList typedSettings = document.getElementsByTagName(type);

                int entryCount = typedSettings.getLength();

                for (int s = 0; s < entryCount ; s++){

                    Node node = typedSettings.item(s);
                    if (node.getNodeType() == Node.ELEMENT_NODE){

                        String name, value;

                        Element nodeElement = (Element) node;

                        NodeList nameList = nodeElement.getElementsByTagName(nameTag);
                        Element nameElement = (Element) nameList.item(0);
                        NodeList textNameList = nameElement.getChildNodes();
                        name = ((Node) textNameList.item(0)).getNodeValue().trim();

                        NodeList valueList = nodeElement.getElementsByTagName(valueTag);
                        Element valueElement = (Element) valueList.item(0);
                        NodeList valueNameList = valueElement.getChildNodes();
                        value = ((Node) valueNameList.item(0)).getNodeValue().trim();

                        if (type.equals(stringTag)){
                            stringSettings.put(name, value);
                        } else if (type.equals(boolTag)){
                            if (value.equals(trueTag)){
                                booleanSettings.put(name, true);
                            } else if (value.equals(falseTag)){
                                booleanSettings.put(name, false);
                            } else {
                                throw new Exception("Incompatible parameters file: Unknown boolean value");
                            }
                        } else if (type.equals(intTag)) {
                            integerSettings.put(name, Integer.valueOf(value));
                        } else if (type.equals(longTag)){
                            longSettings.put(name, Long.valueOf(value));
                        } else {
                            throw new Exception("Incompatible parameters file: Unknown tag");
                        }

                    }

                }
            }


        } catch (SAXParseException err){
            Log.error("[Parameters] Parsing error" + ", line "
                    + err.getLineNumber() + " while reading externally set parameters: " + err.getMessage());

        } catch (SAXException e){
            Exception x = e.getException();
            ((x == null) ? e : x).printStackTrace();

        } catch (Throwable t){
            t.printStackTrace();
        }

        parsed = true;
    }
}
