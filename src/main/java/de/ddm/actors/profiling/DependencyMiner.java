package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.Terminated;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.actor.typed.receptionist.ServiceKey;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.singletons.InputConfigurationSingleton;
import de.ddm.singletons.SystemConfigurationSingleton;
import de.ddm.structures.InclusionDependency;
import de.ddm.structures.TableDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.io.File;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class DependencyMiner extends AbstractBehavior<DependencyMiner.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable, LargeMessageProxy.LargeMessage {
	}

	@NoArgsConstructor
	public static class StartMessage implements Message {
		private static final long serialVersionUID = -1963913294517850454L;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class HeaderMessage implements Message {
		private static final long serialVersionUID = -5322425954432915838L;
		int id;
		String[] header;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BatchMessage implements Message {
		private static final long serialVersionUID = 4591192372652568030L;
		int id;
		List<List<String>> batch;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class RegistrationMessage implements Message {
		private static final long serialVersionUID = -4025238529984914107L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class CompletionMessage implements Message {
		private static final long serialVersionUID = -7642425159675583598L;
		ActorRef<DependencyWorker.Message> dependencyWorker;
		List<TableDependency> dependencies;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyMiner";

	public static final ServiceKey<DependencyMiner.Message> dependencyMinerService = ServiceKey.create(DependencyMiner.Message.class, DEFAULT_NAME + "Service");

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyMiner::new);
	}

	private DependencyMiner(ActorContext<Message> context) {
		super(context);
		this.batches = new ArrayList<>();
		this.discoverNaryDependencies = SystemConfigurationSingleton.get().isHardMode();
		this.inputFiles = InputConfigurationSingleton.get().getInputFiles();
		this.headerLines = new String[this.inputFiles.length][];

		this.inputReaders = new ArrayList<>(inputFiles.length);
		for (int id = 0; id < this.inputFiles.length; id++)
			this.inputReaders.add(context.spawn(InputReader.create(id, this.inputFiles[id]), InputReader.DEFAULT_NAME + "_" + id));
		this.resultCollector = context.spawn(ResultCollector.create(), ResultCollector.DEFAULT_NAME);
		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);

		this.dependencyWorkers = new ArrayList<>();

		context.getSystem().receptionist().tell(Receptionist.register(dependencyMinerService, context.getSelf()));
	}

	/////////////////
	// Actor State //
	/////////////////

	private int batchDeficit = 0;
	private long startTime;
	private int firstJoinPartner = 0;
	private int secJoinPartner = 0;
	private List<BatchMessage> batches;
	private final boolean discoverNaryDependencies;
	private final File[] inputFiles;
	private final String[][] headerLines;

	private final List<ActorRef<InputReader.Message>> inputReaders;
	private final ActorRef<ResultCollector.Message> resultCollector;
	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	private final List<ActorRef<DependencyWorker.Message>> dependencyWorkers;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(StartMessage.class, this::handle)
				.onMessage(BatchMessage.class, this::handle)
				.onMessage(HeaderMessage.class, this::handle)
				.onMessage(RegistrationMessage.class, this::handle)
				.onMessage(CompletionMessage.class, this::handle)
				.onSignal(Terminated.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(StartMessage message) {
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders)
			inputReader.tell(new InputReader.ReadHeaderMessage(this.getContext().getSelf()));
		for (ActorRef<InputReader.Message> inputReader : this.inputReaders){
			inputReader.tell(new InputReader.ReadBatchMessage(this.getContext().getSelf()));
		}
		this.startTime = System.currentTimeMillis();
		return this;
	}

	private Behavior<Message> handle(HeaderMessage message) {
		this.headerLines[message.getId()] = message.getHeader();
		return this;
	}

	private Behavior<Message> handle(BatchMessage message) {
		this.getContext().getLog().info("Got Batch! " + message.getId());

		//Store the batches for later use
		if (message.getBatch().size() != 0)
			batches.add(message);

		return this;
	}

	private Behavior<Message> handle(RegistrationMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		if (!this.dependencyWorkers.contains(dependencyWorker)) {
			this.getContext().getLog().info("Registered Worker!");
			this.dependencyWorkers.add(dependencyWorker);
			this.getContext().watch(dependencyWorker);
			// after the worker is registered he can start working for us until he or the problem is finished :)
			sendBatch(dependencyWorker);
		}
		return this;
	}

	/**
	 * This method keeps track, which table is computed with which table.
	 * We use two int values to represent the combinations and increment the last of them until it reached the end than we increase the first int and repeat.
	 * e.g Tables: 1,2,3,4,5
	 * Combination compute order: (1,2);(1,3);(1,4);(1,5);(2,3);(2,4);(2,5);(3,4);(4,5)
	 * Also we need to wait until all worker finish. For this we use a deficit counter, which counts how many batches we started to compute.
	 * It increases in the start and decreases when we finish a computation.
	 * @param dependencyWorker
	 */
	private void sendBatch(ActorRef<DependencyWorker.Message> dependencyWorker) {
		if(batches.size() != inputReaders.size()){ //some batches are still missing, we wait for them and let the worker idle a bit
			batchDeficit++;
			dependencyWorker.tell(new DependencyWorker.IdleMessage(this.largeMessageProxy));
		} else if(firstJoinPartner >= inputReaders.size() && batchDeficit != 0) { //No batches left to do for us, but some other worker is still working
			return;
		}else if(isFinished()){// last batch is finished we can terminate
			this.end();
		} else {
			BatchMessage batch1 = batches.get(firstJoinPartner);
			BatchMessage batch2 = batches.get(secJoinPartner);
				//increase second int
			secJoinPartner++;
				//if second int is already max, we increase first int and set the second to the first+1
			if (secJoinPartner >= batches.size()) {
				firstJoinPartner++;
				secJoinPartner = firstJoinPartner;
			}
			batchDeficit++;
			this.getContext().getLog().info("Start working on task with " + batch1.getId() + " " + batch2.getId() + " Def: " + batchDeficit);

				//Send working batch to dependency worker, ids are needed for later
			dependencyWorker.tell(new DependencyWorker.TaskMessage(this.largeMessageProxy, batch1.getBatch(), batch2.getBatch(), batch1.getId(), batch2.getId()));
		}
	}

	private Behavior<Message> handle(CompletionMessage message) {
		ActorRef<DependencyWorker.Message> dependencyWorker = message.getDependencyWorker();
		batchDeficit--;
		if (!message.getDependencies().isEmpty() && this.headerLines[0] != null) {
			for (TableDependency dep : message.getDependencies()) {
				this.getContext().getLog().info("Dependency found from " + this.inputFiles[dep.getTableID1()].getName() + " to " + this.inputFiles[dep.getTableID2()].getName());
				int dependent = dep.getTableID1();
				int referenced = dep.getTableID2();
				File dependentFile = this.inputFiles[dependent];
				File referencedFile = this.inputFiles[referenced];
				String[] dependentAttributes = {this.headerLines[dependent][dep.getColID1()]};
				String[] referencedAttributes = {this.headerLines[referenced][dep.getColID2()]};

				InclusionDependency ind = new InclusionDependency(dependentFile, dependentAttributes, referencedFile, referencedAttributes);
				List<InclusionDependency> inds = new ArrayList<>(1);
				inds.add(ind);

				this.resultCollector.tell(new ResultCollector.ResultMessage(inds));
			}
		}

		//Worker can try to start solving again if there are any more batches left
		sendBatch(dependencyWorker);

		if(isFinished())
			this.end();
		return this;
	}

	private boolean isFinished() {
		return firstJoinPartner >= inputReaders.size() && batchDeficit == 0;
	}

	private void end() {
		this.resultCollector.tell(new ResultCollector.FinalizeMessage());
		long discoveryTime = System.currentTimeMillis() - this.startTime;
		this.getContext().getLog().info("Finished mining within {} ms!", discoveryTime);
	}

	private Behavior<Message> handle(Terminated signal) {
		ActorRef<DependencyWorker.Message> dependencyWorker = signal.getRef().unsafeUpcast();
		this.dependencyWorkers.remove(dependencyWorker);
		return this;
	}
}