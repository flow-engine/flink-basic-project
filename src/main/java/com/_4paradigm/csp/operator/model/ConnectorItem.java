package com._4paradigm.csp.operator.model;

import com._4paradigm.pdms.telamon.model.CommonSchemaTerm;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;

@Data
public class ConnectorItem {
    private String name;
    private String prn;
    private List<CommonSchemaTerm> groupSchema = new ArrayList<>();
    private List<StreamConfigItem> streamConfigItems = new ArrayList<>();
}
