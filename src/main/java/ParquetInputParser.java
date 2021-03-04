import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import java.io.File;
import java.io.IOException;

@Slf4j
public class ParquetInputParser implements InputParser {

    ParquetReader<GenericData.Record> reader;
    Schema schema;
    String[] header;
    File inputFile;

    public ParquetInputParser() {
    }

    @Override
    public void beginParsing(File inputFile) {
        this.inputFile = inputFile;
        log.info("Using parquet reader for {}", inputFile);
        try {
            Path path = new Path(inputFile.toString());
            Configuration conf = new Configuration();
            InputFile filePath = HadoopInputFile.fromPath(path, conf);
            try (ParquetReader<GenericData.Record> schemaReader = AvroParquetReader
                    .<GenericData.Record>builder(filePath).build()) {
                schema = schemaReader.read().getSchema();
                header = schemaReader.read().getSchema().getFields().stream()
                        .map(Schema.Field::name).toArray(String[]::new);
            }
            reader = AvroParquetReader
                    .<GenericData.Record>builder(filePath)
                    .build();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public Schema getSchema(){
        return schema;
    }

    @Override
    public String[] getHeader() {
        return header;
    }

    @Override
    public ParquetInputRecord parseNextRecord() {
        GenericData.Record record;
        try {
            record = reader.read();
        } catch (IOException e) {
            log.error("Error reading " + inputFile, e);
            throw new RuntimeException(e);
        }
        if (record == null) {
            return null;
        }
        return new ParquetInputRecord(header, record);
    }

    @Override
    public void stopParsing() {
        try {
            reader.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
}
