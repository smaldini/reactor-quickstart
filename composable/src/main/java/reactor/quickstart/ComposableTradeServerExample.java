package reactor.quickstart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Composable;
import reactor.fn.Function;

import java.util.concurrent.TimeUnit;

/**
 * @author Jon Brisbin
 */
public class ComposableTradeServerExample {

	public static void main(String[] args) throws InterruptedException {
		final TradeServer server = new TradeServer();

		// Rather than handling Trades as events, each Trade is accessible via Composable.
		Composable<Trade> trades = new Composable<>();
		// We can always set a length to a Composable if we know it (completely optional).
		trades.setExpectedAcceptCount(totalTrades);

		// We compose an action to turn a Trade into an Order by calling server.execute(Trade).
		Composable<Order> orders = trades.map(new Function<Trade, Order>() {
			@Override
			public Order apply(Trade trade) {
				return server.execute(trade);
			}
		});

		// Start a throughput timer.
		startTimer();

		// Publish one event per trade.
		for (int i = 0; i < totalTrades; i++) {
			// Pull next randomly-generated Trade from server into the Composable,
			Trade trade = server.nextTrade();
			// Notify the Composable this Trade is ready to be executed
			trades.accept(trade);
		}

		// Composables can block until all values have passed through them.
		// They know when the end has arrived because we set the length earlier.
		orders.await(30, TimeUnit.SECONDS);

		// Stop throughput timer and output metrics.
		endTimer();

		server.stop();
	}

	private static void startTimer() {
		LOG.info("Starting throughput test with {} trades...", totalTrades);
		startTime = System.currentTimeMillis();
	}

	private static void endTimer() throws InterruptedException {
		endTime = System.currentTimeMillis();
		elapsed = (endTime - startTime) * 1.0;
		throughput = totalTrades / (elapsed / 1000);

		LOG.info("Executed {} trades/sec in {}ms", (int) throughput, (int) elapsed);
	}

	private static final Logger LOG         = LoggerFactory.getLogger(ComposableTradeServerExample.class);
	private static       int    totalTrades = 5000000;
	private static long   startTime;
	private static long   endTime;
	private static double elapsed;
	private static double throughput;

}
