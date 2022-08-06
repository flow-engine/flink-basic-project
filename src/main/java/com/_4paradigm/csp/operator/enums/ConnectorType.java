package com._4paradigm.csp.operator.enums;

import com.fasterxml.jackson.annotation.JsonCreator;

public enum ConnectorType {

	KAFKA("KAFKA"),
	JDBC("JDBC"),
	ELASTICSEARCH("ELASTICSEARCH"),
	RTIDB("RTIDB"),
	OTHER("OTHER");

	private final String code;

	public String getCode(){
		return this.code;
	}

	ConnectorType(String code) {
		this.code = code;
	}

	public static ConnectorType valueOfEnum(String code) {
		for (ConnectorType connectorType : ConnectorType.values()) {
			if (connectorType.code.equals(code)) {
				return connectorType;
			}
		}
		return null;
	}

	@JsonCreator
	public static ConnectorType deserializeEnum(String code) {
		for (ConnectorType connectorType : ConnectorType.values()) {
			if (connectorType.code.equals(code)) {
				return connectorType;
			}
		}
		return null;
	}
	
}
