package testisw2.util;

import java.util.Enumeration;
import java.util.concurrent.CountDownLatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.apache.bookkeeper.client.LedgerEntry;
import org.apache.bookkeeper.client.LedgerHandle;

import io.netty.buffer.ByteBuf;

public class CustomReadCallback implements ReadCallback {
	private int result= 2;
	private CountDownLatch latch;
	private Enumeration<LedgerEntry> entries;
	public Enumeration<LedgerEntry> getLedgerEntries(){
		return this.entries;
	}
	public void setCountDownLatch(CountDownLatch latch) {
		this.latch = latch;
	}
	public int getRc() {
		return this.result;
	}
	@Override
	public void readComplete(int rc, LedgerHandle lh, Enumeration<LedgerEntry> seq, Object ctx) {
		this.result = rc;
		if(ctx instanceof CustomContextObject) {
			CustomContextObject customCtx = (CustomContextObject) ctx;
			customCtx.setLedgerHandle(lh);
		
		}
		this.entries = seq;
		this.latch.countDown();
	}

}
