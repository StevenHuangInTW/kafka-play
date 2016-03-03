package org.jbaiera.play.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;

public class SoutSink extends AbstractSink implements Configurable {

	private static final Logger LOGGER = LoggerFactory.getLogger(SoutSink.class);

	private PrintStream outputStream;
	private boolean printHeaders;
	private long outputCount;

	@Override
	public void configure(Context context) {
		// process context of input values:
		this.printHeaders = "true".equalsIgnoreCase(context.getString("printHeaders", "false"));
		LOGGER.info("Found configured property value : '{}'", this.printHeaders);
	}

	@Override
	public synchronized void start() {
		// Initialize your connection to external resource:
		this.outputStream = System.out;
	}

	@Override
	public synchronized void stop() {
		// Cleanup any allocated resources:
		// (Null out the stream because we really shouldn't close STDOUT)
		this.outputStream = null;

		// Let's log how many messages we received:
		LOGGER.info("Received {} messages", outputCount);
	}

	@Override
	public Status process() throws EventDeliveryException {
        // We're optimistic to start:
        Status returnStatus = Status.READY;

        // Get the channel and the next transaction:
        Channel ch = getChannel();
        Transaction txn = ch.getTransaction();

        try {
            // Do everything in a try catch:
            txn.begin();
            Event event = ch.take();

            // Events can be null if there's nothing in the queue at the moment...
            if (event != null) {
                // Process the event:
                doSomething(event);
            } else {
                // Didn't pick up an event? Signal runner to back off:
                returnStatus = Status.BACKOFF;
            }

            txn.commit();

        } catch (Throwable t) {
            // Throwable caught. Rollback current Flume transaction:
            txn.rollback();

            // Potentially handle the exception if it's something recoverable
            potentiallyHandleException(t);

            // Signal runner to back off:
            returnStatus = Status.BACKOFF;

            // Always re-throw Errors!
            if (t instanceof Error) {
                throw (Error)t;
            }
        } finally {
            // Always close the txn at the end!
            txn.close();
        }

        return returnStatus;
	}

    private void doSomething(Event event) {
        // Here we're just going to convert the bytes to strings and
        // write them to STDOUT:

		if (printHeaders) {
			String headers = event.getHeaders().toString();
			outputStream.println(headers);
		}

        String data = new String(event.getBody());
        outputStream.println(data);
		outputCount++;
    }

    private void potentiallyHandleException(Throwable t) {
        // In this case we're just going to log it:
        LOGGER.error("SoutSink encountered exception.", t);
    }
}
