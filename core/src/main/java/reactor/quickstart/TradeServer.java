package reactor.quickstart;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Jon Brisbin
 */
public class TradeServer {

	private static final Logger               LOG        = LoggerFactory.getLogger(TradeServer.class);
	private static final String               CHARS      = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static final int                  LEN        = CHARS.length();
	private final        Random               random     = new Random();
	private final        AtomicLong           counter    = new AtomicLong();
	private final        Thread               queueDrain = new Thread() {
		@Override
		public void run() {
			while (active.get()) {
				Order o;
				try {
					// Pull Orders off the queue and process them
					o = buys.poll(100, TimeUnit.MILLISECONDS);
					o = sells.poll(100, TimeUnit.MILLISECONDS);
				} catch (InterruptedException e) {
				}
			}
		}
	};
	final                BlockingQueue<Order> buys       = new LinkedBlockingQueue<Order>();
	final                BlockingQueue<Order> sells      = new LinkedBlockingQueue<Order>();
	final                AtomicBoolean        active     = new AtomicBoolean(true);

	public TradeServer() {
		queueDrain.start();
	}

	public Order execute(Trade trade) {
		Order o = new Order(counter.incrementAndGet())
				.setTrade(trade)
				.setTimestamp(System.currentTimeMillis());

		switch (trade.getType()) {
			case BUY:
				buys.add(o);
				break;
			case SELL:
				sells.add(o);
				break;
		}

		return o;
	}

	public Trade nextTrade() {
		return new Trade(counter.incrementAndGet())
				.setSymbol(nextSymbol())
				.setQuantity(random.nextInt(500))
				.setPrice(Float.parseFloat(random.nextInt(700) + "." + random.nextInt(99)))
				.setType((random.nextInt() % 2 == 0 ? Type.BUY : Type.SELL));
	}

	public void stop() {
		active.set(false);
	}

	private String nextSymbol() {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < 4; i++) {
			sb.append(CHARS.charAt(random.nextInt(LEN)));
		}
		return sb.toString();
	}

}
