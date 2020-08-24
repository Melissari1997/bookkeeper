package org.apache.bookkeeper.client.impl;

/*
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
 */


import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import com.google.common.collect.Lists;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.bookkeeper.client.api.LedgerEntry;
import org.apache.bookkeeper.client.impl.LedgerEntriesImpl;
import org.apache.bookkeeper.client.impl.LedgerEntryImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for {@link LedgerEntriesImpl}.
 * @author Paolo Melissari
 */
public class LedgerEntriesImplTest {
    private final int entryNumber = 2;
    private LedgerEntriesImpl ledgerEntriesImpl;
    private final List<LedgerEntry> entryList = Lists.newArrayList();

    // content for each entry
    private final long ledgerId = 1234L;
    private final byte[] dataBytes = "test-ledger-entry-impl".getBytes(UTF_8);
    private final ArrayList<ByteBuf> bufs = Lists.newArrayListWithExpectedSize(entryNumber);
    @Before
    public void setup() {
        for (int i = 0; i < entryNumber; i++) {
            ByteBuf buf = Unpooled.wrappedBuffer(dataBytes);
            bufs.add(buf);
            entryList.add(LedgerEntryImpl.create(ledgerId,
                i,
                dataBytes.length,
                buf));
        }

        ledgerEntriesImpl = LedgerEntriesImpl.create(entryList);
    }

    @After
    public void tearDown() {
        ledgerEntriesImpl.close();

        // References should be released after close.
        bufs.forEach(byteBuf -> assertEquals(0, byteBuf.refCnt()));

        try {
            ledgerEntriesImpl.iterator();
            fail("Should fail iterator after close");
        } catch (NullPointerException e) {
            // expected behavior
        }
    }

    @Test
    public void testGetEntry() {

        LedgerEntry actualResult = ledgerEntriesImpl.getEntry(0);
        assertEquals(entryList.get(0).getLedgerId(),  actualResult.getLedgerId());
        assertEquals(entryList.get(0).getEntryId(),  actualResult.getEntryId());
        assertEquals(entryList.get(0).getLength(),  actualResult.getLength());
        
        actualResult = ledgerEntriesImpl.getEntry(1);
        assertEquals(entryList.get(1).getLedgerId(),  actualResult.getLedgerId());
        assertEquals(entryList.get(1).getEntryId(),  actualResult.getEntryId());
        assertEquals(entryList.get(1).getLength(),  actualResult.getLength());
        
        try{
        	actualResult = ledgerEntriesImpl.getEntry(-1);
        	fail("Should get IndexOutOfBoundsException");
        }catch(Exception e) {
        	
        }
        
    }
    
    @Test
    public void testCreate() {
    	try{
    		LedgerEntriesImpl emptyLedgersEntry = LedgerEntriesImpl.create(null); 
    		fail("Should not create from null");
    	}
    	catch(Exception e){
    		//correct behavior
    	}
    	List<LedgerEntry> entryList = Lists.newArrayList();
    	try{
    		LedgerEntriesImpl emptyLedgersEntry = LedgerEntriesImpl.create(entryList); 
    		fail("Should not create an empty list");
    	}
    	catch(Exception e){
    		//correct behavior
    	}
    	
    	entryList.add(LedgerEntryImpl.create(0, 0));
    	LedgerEntriesImpl actualLedgerEntries = LedgerEntriesImpl.create(entryList);
    	int size = 0;
    	 Iterator<LedgerEntry> iterator = actualLedgerEntries.iterator();
    	while(iterator.hasNext()) {
    		size++;
    		iterator.next();
    	}
    	assertEquals(entryList.size(),size);
    	assertEquals(entryList.get(0).getClass(), actualLedgerEntries.getEntry(0).getClass());
    	assertEquals(entryList.get(0),actualLedgerEntries.getEntry(0));
    }
}
