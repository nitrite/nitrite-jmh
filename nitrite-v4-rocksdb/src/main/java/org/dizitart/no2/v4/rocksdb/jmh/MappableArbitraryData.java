package org.dizitart.no2.v4.rocksdb.jmh;

import lombok.Data;
import lombok.experimental.Accessors;
import org.dizitart.no2.collection.Document;
import org.dizitart.no2.mapper.Mappable;
import org.dizitart.no2.mapper.NitriteMapper;
import org.dizitart.no2.repository.annotations.Id;

/**
 * @author Anindya Chatterjee
 */
@Data
@Accessors(fluent = true, chain = true)
public class MappableArbitraryData implements Mappable {
    @Id
    private Integer id;
    private String text;
    private Double number1;
    private Double number2;
    private Integer index1;
    private Boolean flag1;
    private Boolean flag2;

    @Override
    public Document write(NitriteMapper nitriteMapper) {
        return Document.createDocument("id", id)
                .put("text", text)
                .put("number1", number1)
                .put("number2", number2)
                .put("index1", index1)
                .put("flag1", flag1)
                .put("flag2", flag2);
    }

    @Override
    public void read(NitriteMapper nitriteMapper, Document document) {
        id = document.get("id", Integer.class);
        text = document.get("text", String.class);
        number1 = document.get("number1", Double.class);
        number2 = document.get("number2", Double.class);
        index1 = document.get("index1", Integer.class);
        flag1 = document.get("flag1", Boolean.class);
        flag2 = document.get("flag2", Boolean.class);
    }
}
