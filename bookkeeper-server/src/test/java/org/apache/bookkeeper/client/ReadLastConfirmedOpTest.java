package org.apache.bookkeeper.client;

import com.google.common.collect.Lists;
import io.netty.buffer.UnpooledByteBufAllocator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.net.BookieSocketAddress;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat.DigestType;
import org.apache.bookkeeper.proto.checksum.DigestManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests about ReadLastConfirmedOp.
 */
public class ReadLastConfirmedOpTest {
    private static final Logger log = LoggerFactory.getLogger(ReadLastConfirmedOpTest.class);
    private final BookieSocketAddress bookie1 = new BookieSocketAddress("bookie1", 3181);
    private final BookieSocketAddress bookie2 = new BookieSocketAddress("bookie2", 3181);

    OrderedExecutor executor = null;

    @Before
    public void setup() throws Exception {
        executor = OrderedExecutor.newBuilder()
                .name("BookKeeperClientWorker")
                .numThreads(1)
                .build();
    }

    @After
    public void teardown() throws Exception {
        if (executor != null) {
            executor.shutdown();
        }
    }

    /**
     * Test for specific bug that was introduced with dcdd1e88.
     */
    @Test
    public void testBookieFailsAfterLedgerMissingOnFirst() throws Exception {
        long ledgerId = 0xf00b;
        List<BookieSocketAddress> ensemble = Lists.newArrayList(bookie1, bookie2);
        byte[] ledgerKey = new byte[0];

        MockBookieClient bookieClient = new MockBookieClient(executor);
        DistributionSchedule schedule = new RoundRobinDistributionSchedule(2, 2, 2);
        DigestManager digestManager = DigestManager.instantiate(ledgerId, ledgerKey,
                                                                DigestType.CRC32C,
                                                                UnpooledByteBufAllocator.DEFAULT,
                                                                true /* useV2 */);

        CompletableFuture<Void> blocker = new CompletableFuture<>();
        bookieClient.setPreReadHook((bookie, lId, entryId) -> {
                if (bookie.equals(bookie1)) {
                    return CompletableFuture.completedFuture(null);
                } else {
                    return blocker;
                }
            });
        CompletableFuture<DigestManager.RecoveryData> promise = new CompletableFuture<>();
        ReadLastConfirmedOp op = new ReadLastConfirmedOp(
                bookieClient, schedule,
                digestManager, ledgerId, ensemble,
                ledgerKey,
                (rc, data) -> {
                    if (rc != BKException.Code.OK) {
                        promise.completeExceptionally(
                                BKException.create(rc));
                    } else {
                        promise.complete(data);
                    }
                });
        op.initiateWithFencing();

        while (op.getNumResponsesPending() > 1) {
            Thread.sleep(100);
        }
        blocker.completeExceptionally(
                new BKException.BKBookieHandleNotAvailableException());
        promise.get();
    }
}