package com._4paradigm.csp.operator.enums;

public enum SyncEsType {
    /**
     * 全量同步
     * * 需要增加版本号
     */
    full,
    /**
     * 增量同步: 只需要同步更新(有记录, Merge 更新; 没有记录: 塞入)
     * * Merge 更新
     * * 需要增加版本号
     */
    incremental,
    /**
     * 限定同步: 只有在es对应记录存在才要更新
     * Merge 更新
     */
    finite
}
