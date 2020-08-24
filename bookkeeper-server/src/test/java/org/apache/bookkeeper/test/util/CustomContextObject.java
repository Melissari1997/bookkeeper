package org.apache.bookkeeper.test.util;

import org.apache.bookkeeper.client.LedgerHandle;

public class CustomContextObject {
	private LedgerHandle lh;
	
	public void setLedgerHandle(LedgerHandle lh) {
		this.lh = lh;
	}
	
	public LedgerHandle getLedgerHandle() {
		return this.lh;
	}
	
}
