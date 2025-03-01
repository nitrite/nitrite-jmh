package org.dizitart.no2.v4.jmh;

import lombok.Data;
import lombok.experimental.Accessors;
import org.dizitart.no2.collection.Document;
import org.dizitart.no2.common.mapper.EntityConverter;
import org.dizitart.no2.common.mapper.NitriteMapper;
import org.dizitart.no2.repository.annotations.Id;

/**
 * @author Anindya Chatterjee
 */
@Data
@Accessors(fluent = true, chain = true)
public class ArbitraryData {
    @Id
    private Integer id;
    private String text;
    private Double number1;
    private Double number2;
    private Integer index1;
    private Boolean flag1;
    private Boolean flag2;

    public static class Converter implements EntityConverter<ArbitraryData> {

        @Override
        public Class<ArbitraryData> getEntityType() {
            return ArbitraryData.class;
        }

        @Override
        public Document toDocument(ArbitraryData obj, NitriteMapper nitriteMapper) {
            return Document.createDocument("id", obj.id())
                    .put("text", obj.text())
                    .put("number1", obj.number1())
                    .put("number2", obj.number2())
                    .put("index1", obj.index1())
                    .put("flag1", obj.flag1())
                    .put("flag2", obj.flag2());
        }

        @Override
        public ArbitraryData fromDocument(Document document, NitriteMapper nitriteMapper) {
            ArbitraryData arbitraryData = new ArbitraryData();
            arbitraryData.id(document.get("id", Integer.class));
            arbitraryData.text(document.get("text", String.class));
            arbitraryData.number1(document.get("number1", Double.class));
            arbitraryData.number2(document.get("number2", Double.class));
            arbitraryData.index1(document.get("index1", Integer.class));
            arbitraryData.flag1(document.get("flag1", Boolean.class));
            arbitraryData.flag2(document.get("flag2", Boolean.class));
            return arbitraryData;
        }
    }
}
