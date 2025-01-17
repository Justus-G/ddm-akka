package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.PostStop;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.DomainConfigurationSingleton;
import de.ddm.singletons.InputConfigurationSingleton;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class InputReader extends AbstractBehavior<InputReader.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadHeaderMessage implements Message {
		private static final long serialVersionUID = 1729062814525657711L;
		ActorRef<DependencyMiner.Message> replyTo;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReadBatchMessage implements Message {
		private static final long serialVersionUID = -7915854043207237318L;
		ActorRef<DependencyMiner.Message> replyTo;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "inputReader";

	public static Behavior<Message> create(final int id, final File inputFile) {
		return Behaviors.setup(context -> new InputReader(context, id, inputFile));
	}

	private InputReader(ActorContext<Message> context, final int id, final File inputFile) throws IOException, CsvValidationException {
		super(context);
		this.id = id;
		this.reader = InputConfigurationSingleton.get().createCSVReader(inputFile);
		this.header = InputConfigurationSingleton.get().getHeader(inputFile);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final int id;
	private final int batchSize = DomainConfigurationSingleton.get().getInputReaderBatchSize();
	private final CSVReader reader;
	private final String[] header;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReadHeaderMessage.class, this::handle)
				.onMessage(ReadBatchMessage.class, this::handle)
				.onSignal(PostStop.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReadHeaderMessage message) {
		message.getReplyTo().tell(new DependencyMiner.HeaderMessage(this.id, this.header));
		return this;
	}

	private Behavior<Message> handle(ReadBatchMessage message) throws IOException, CsvValidationException {
		List<String[]> batch = new ArrayList<>(this.batchSize);
		for (int i = 0; i < this.batchSize; i++) {
			String[] line = this.reader.readNext();
			if (line == null)
				break;
			batch.add(line);
		}
		//return the table, after its transformed to fit the column store type.
		List<List<String>> transposedBatch = transposeBatch(batch);
		message.getReplyTo().tell(new DependencyMiner.BatchMessage(this.id, transposedBatch));
		return this;
	}

	/**
	 * We transpose the current Table, because it stores rows, which makes it quite hard to compute the INDs.
	 * With the transpose we go from storing rows to columns.
	 * @param batch
	 * @return
	 */
	private List<List<String>> transposeBatch(List<String[]> batch) {
		List<List<String>> ret = new ArrayList<>();
		final int N = batch.get(0).length;
		for (int i = 0; i < N; i++) {
			List<String> col = new ArrayList<>();
			for (String[] row : batch) {
				col.add(row[i]);
			}
			col.remove(0);
			ret.add(col);
		}
		return ret;
	}

	private Behavior<Message> handle(PostStop signal) throws IOException {
		this.reader.close();
		return this;
	}
}