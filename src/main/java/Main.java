import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

@Slf4j
public class Main {

    private Schema schema;

    @Option(name = "--file", required = true, usage = "Parquet file to convert")
    private String parquetFile = "";

    public static void main(String[] args) {
        Main converter = new Main();
        CmdLineParser parser = new CmdLineParser(converter);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            parser.printUsage(System.err);
            System.exit(1);
        }
        converter.run();
    }

    public void run(){
        ParquetInputParser parquetParser = new ParquetInputParser();
        parquetParser.beginParsing(new File(parquetFile));
        schema = parquetParser.getSchema();
        writeSchemaFile();
        try(FileWriter writer = new FileWriter(parquetFile + ".csv")){

            // Write the header part of csv
            StringBuilder sb = new StringBuilder();
            String[] parquetHeader = parquetParser.getHeader();
            for(int i = 0; i < parquetHeader.length; i++){
                if(i != parquetHeader.length - 1){
                    sb.append(parquetHeader[i] + ",");
                }
                else{
                    sb.append(parquetHeader[i] + "\n");
                }
            }
            writer.write(sb.toString());
            ParquetInputRecord record = parquetParser.parseNextRecord();

            //write all the records
            while(record != null){
                sb = new StringBuilder();
                String[] recordvalues = record.getValues();
                for(int i = 0; i < recordvalues.length; i++){
                    if(i != recordvalues.length - 1){
                        sb.append(recordvalues[i] + ",");
                    }
                    else{
                        sb.append(recordvalues[i] + "\n");
                    }
                }
                writer.write(sb.toString());
                record = parquetParser.parseNextRecord();
            }
        }
        catch(IOException e){
            log.info(e.getMessage());
        }
    }

    private Schema.Type getSchemaType(Schema subschema) {
        if (subschema.getType() == Schema.Type.UNION) {
            return subschema.getTypes().get(1).getType();
        } else {
            return subschema.getType();
        }
    }

    private void writeSchemaFile(){
        JSONObject jo = new JSONObject();
        jo.put("name","GmpcLocationEvent");
        jo.put("namespace","com.reach_u.mw.locationstream.mhparquet.model");
        jo.put("type","record");
        JSONArray JArray = new JSONArray();
        for (Schema.Field field: schema.getFields()){
            JSONObject fieldJO = new JSONObject();
            fieldJO.put("name", field.name());
            Schema.Type fieldSchema = field.schema().getType();
            if(fieldSchema == Schema.Type.UNION){
                JSONArray fieldTypes = new JSONArray();
                fieldTypes.add("null");
                fieldTypes.add(getSchemaType(field.schema()).getName());
                fieldJO.put("type", fieldTypes);
            }
            else{
                fieldJO.put("type", getSchemaType(field.schema()).getName());
            }
            JArray.add(fieldJO);
        }
        jo.put("fields", JArray);
        try(FileWriter writer = new FileWriter(parquetFile + "_schema.avsc")){
            writer.write(jo.toJSONString());
            log.info("Schema file written succesfully");
        }
        catch(IOException e){
            log.info(e.getMessage());
        }
    }
}
