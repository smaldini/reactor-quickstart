package reactor.quickstart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.Fn;
import reactor.core.Reactor;
import reactor.fn.Consumer;
import reactor.fn.Event;
import reactor.fn.Selector;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static reactor.Fn.$;

/**
 * @author Jon Brisbin
 */
public class TradeServerExample {

	public static void main(String[] args) throws InterruptedException {
		final TradeServer server = new TradeServer();

		// Use a Reactor to dispatch events using the default Dispatcher
		Reactor reactor = new Reactor();

		// Create a single Selector for efficiency
		Selector trade = $("trade.execute");

		// For each Trade event, execute that on the server
		reactor.on(trade, new Consumer<Event<Trade>>() {
			@Override
			public void accept(Event<Trade> tradeEvent) {
				server.execute(tradeEvent.getData());

				// Since we're async, for this test, use a latch to tell when we're done
				latch.countDown();
			}
		});

		// Start a throughput timer
		startTimer();

		// Publish one event per trade
		for (int i = 0; i < totalTrades; i++) {
			// Pull next randomly-generated Trade from server
			Trade t = server.nextTrade();

			// Notify the Reactor the event is ready to be handled
			reactor.notify(trade, Fn.event(t));
		}

		// Stop throughput timer and output metrics
		endTimer();

		server.stop();
	}

	private static void startTimer() {
		LOG.info("Starting throughput test with {} trades...", totalTrades);
		latch = new CountDownLatch(totalTrades);
		startTime = System.currentTimeMillis();
	}

	private static void endTimer() throws InterruptedException {
		latch.await(30, TimeUnit.SECONDS);
		endTime = System.currentTimeMillis();
		elapsed = (endTime - startTime) * 1.0;
		throughput = totalTrades / (elapsed / 1000);

		LOG.info("Executed {} trades/sec in {}ms", (int) throughput, (int) elapsed);
	}

	private static final Logger LOG = LoggerFactory.getLogger(TradeServerExample.class);
	private static CountDownLatch latch;
	private static int totalTrades = 5000000;
	private static long   startTime;
	private static long   endTime;
	private static double elapsed;
	private static double throughput;

}
