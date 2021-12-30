package de.ddm.actors.profiling;

import akka.actor.typed.ActorRef;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.actor.typed.receptionist.Receptionist;
import akka.japi.Pair;
import de.ddm.actors.patterns.LargeMessageProxy;
import de.ddm.serialization.AkkaSerializable;
import de.ddm.structures.TableDependency;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.*;

public class DependencyWorker extends AbstractBehavior<DependencyWorker.Message> {

	////////////////////
	// Actor Messages //
	////////////////////

	public interface Message extends AkkaSerializable {
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class ReceptionistListingMessage implements Message {
		private static final long serialVersionUID = -5246338806092216222L;
		Receptionist.Listing listing;
	}

	@Getter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class TaskMessage implements Message {
		private static final long serialVersionUID = -4667745204456518160L;
		ActorRef<LargeMessageProxy.Message> dependencyMinerLargeMessageProxy;
		List<List<String>> batchFirst;
		List<List<String>> batchSec;
		int table1ID;
		int table2ID;
	}

	////////////////////////
	// Actor Construction //
	////////////////////////

	public static final String DEFAULT_NAME = "dependencyWorker";

	public static Behavior<Message> create() {
		return Behaviors.setup(DependencyWorker::new);
	}

	private DependencyWorker(ActorContext<Message> context) {
		super(context);

		final ActorRef<Receptionist.Listing> listingResponseAdapter = context.messageAdapter(Receptionist.Listing.class, ReceptionistListingMessage::new);
		context.getSystem().receptionist().tell(Receptionist.subscribe(DependencyMiner.dependencyMinerService, listingResponseAdapter));

		this.largeMessageProxy = this.getContext().spawn(LargeMessageProxy.create(this.getContext().getSelf().unsafeUpcast()), LargeMessageProxy.DEFAULT_NAME);
	}

	/////////////////
	// Actor State //
	/////////////////

	private final ActorRef<LargeMessageProxy.Message> largeMessageProxy;

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive<Message> createReceive() {
		return newReceiveBuilder()
				.onMessage(ReceptionistListingMessage.class, this::handle)
				.onMessage(TaskMessage.class, this::handle)
				.build();
	}

	private Behavior<Message> handle(ReceptionistListingMessage message) {
		Set<ActorRef<DependencyMiner.Message>> dependencyMiners = message.getListing().getServiceInstances(DependencyMiner.dependencyMinerService);
		for (ActorRef<DependencyMiner.Message> dependencyMiner : dependencyMiners)
			dependencyMiner.tell(new DependencyMiner.RegistrationMessage(this.getContext().getSelf()));
		return this;
	}

	private Behavior<Message> handle(TaskMessage message) {
		this.getContext().getLog().info("Working!");
		// I should probably know how to solve this task, but for now I just pretend some work...

		List<List<String>> batch1 = message.getBatchFirst();
		List<List<String>> batch2 = message.getBatchSec();

		int x = 0;
		int y = 0;
		List<TableDependency> dependencies = new ArrayList<>();
		for (List<String> col : batch1){
			y = 0;
			for (List<String> col2: batch2){
				if(col.containsAll(col2)){
					dependencies.add(new TableDependency(message.getTable2ID(), message.getTable1ID(), y, x));
				} else if(col2.containsAll(col)){
					dependencies.add(new TableDependency(message.getTable1ID(), message.getTable2ID(), x, y));
				}
				y++;
			}
			x++;
		}

		LargeMessageProxy.LargeMessage completionMessage = new DependencyMiner.CompletionMessage(this.getContext().getSelf(), dependencies);
		this.largeMessageProxy.tell(new LargeMessageProxy.SendMessage(completionMessage, message.getDependencyMinerLargeMessageProxy()));
		return this;
	}
}
