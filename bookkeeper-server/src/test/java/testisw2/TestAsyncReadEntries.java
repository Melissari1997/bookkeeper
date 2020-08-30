package testisw2;

import static org.junit.Assert.assertEquals;

/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.BookKeeper.DigestType;
import org.apache.bookkeeper.client.LedgerHandle;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.client.AsyncCallback.ReadCallback;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import testisw2.util.BookkeeperSetup;
import testisw2.util.CustomContextObject;
import testisw2.util.CustomReadCallback;

import org.apache.zookeeper.ZooKeeper;
/**
 * Test Create/Delete ledgers.
 */
@RunWith(Parameterized.class)
public class TestAsyncReadEntries extends BookkeeperSetup {
	private ReadCallback customReadCallback;
	private Object customContextObject;
	private long firstEntry;
	private long lastEntry;
	private int expectedResult;
	
	
    public TestAsyncReadEntries(long firstEntry, long lastEntry, ReadCallback cb, Object ctx, int expectedResult) throws Exception {
        super();
        this.firstEntry = firstEntry;
        this.lastEntry = lastEntry;
        this.customReadCallback =cb;
        this.customContextObject = ctx;
        this.expectedResult = expectedResult; 
        
    }


    @Before
    public void setUp() throws Exception {
        baseConf.setOpenFileLimit(1);
        super.setUp();
        this.customContextObject = new CustomContextObject();
    }
    
    	
    	@Parameterized.Parameters
        public static Collection<Object[]> ClassLoaderParameters() {
        	return Arrays.asList(new Object[][] {
        		//Test suite minimale
        		{-1L, 0L, null, null,0},
        		{0L,-1L, new CustomReadCallback(), new CustomContextObject(),BKException.Code.IncorrectParameterException}, //line 683 LedgerHandle
        		{1L,1L, new CustomReadCallback(), new CustomContextObject(),BKException.Code.OK},
        		//adeguacy
        		{-1L,-2L, new CustomReadCallback(), new CustomContextObject(),BKException.Code.IncorrectParameterException}, 
        		{0L,6L,new CustomReadCallback(), new CustomContextObject(),BKException.Code.ReadException}, 
        		//mutation. lastEntry = lastAddConfirmed
        		{0L,4L,new CustomReadCallback(), new CustomContextObject(),BKException.Code.OK}, 
        		});
        };
        
        @Test
        public void testAsyncReadEntries() throws Exception {
        	LedgerHandle lh = bkc.createLedger(1, 1, DigestType.CRC32, "bk is cool".getBytes());
            CountDownLatch counter = new CountDownLatch(1);
            
        	for (int j = 0; j < 5; j++) {
                lh.addEntry("just test".getBytes());
              
            }
            try{
            	((CustomReadCallback)this.customReadCallback).setCountDownLatch(counter);
            	lh.asyncReadEntries(this.firstEntry, this.lastEntry, this.customReadCallback, this.customContextObject);
            	
            }catch(Exception e) {
            	if(this.customReadCallback == null) {
            		return;
            	}
            	else {
            		fail("Should not fail when ReadCallback is not null");
            	}
            }
            counter.await();
            
            assertEquals(this.expectedResult, ((CustomReadCallback)this.customReadCallback).getRc());
            assertEquals(lh, ((CustomContextObject)this.customContextObject).getLedgerHandle());
        }
      
   

}
