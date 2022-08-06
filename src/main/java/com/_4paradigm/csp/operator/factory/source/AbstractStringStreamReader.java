package com._4paradigm.csp.operator.factory.source;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;

public abstract class AbstractStringStreamReader extends AbstractDataSourceReader {

    @Override
    public TypeInformation getTypeInformation() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
