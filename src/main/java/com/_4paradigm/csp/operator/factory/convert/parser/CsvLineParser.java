package com._4paradigm.csp.operator.factory.convert.parser;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class CsvLineParser extends AbstractLineParser {

    private CSVFormat csvFormat;

    @Override
    public List<String> parse(String line) {
        List<String> items = new ArrayList<>();
        try {
            Iterable<CSVRecord> records = csvFormat.parse(new StringReader(line));
            CSVRecord record = records.iterator().next();
            for (String item : record) {
                items.add(item);
            }
        } catch (Exception ex) {
            throw new RuntimeException("Invalid line can't be parse: " + line, ex);
        }
        return items;
    }
}
