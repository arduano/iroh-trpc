/*****************************************************************************
 * 1) Imports & Type Declarations
 *****************************************************************************/

import {
  Iroh,
  NodeAddr,
  AuthorId,
  DocTicket,
  ShareMode,
  AddrInfoOptions,
  Query,
} from "@number0/iroh";
import { randomUUID } from "node:crypto";

export type WorkInstruction = string;

interface TaskRecord {
  id: string; // GUID
  instruction: WorkInstruction; // The actual work request
  assignedTo?: string; // Node ID that is doing the work
  completed?: boolean;
  resultDoc?: string; // DocTicket as string
}

interface IRaftCommand {
  term: number; // The Raft term in which this command was inserted
  type: "CREATE" | "ASSIGN" | "COMPLETE";
  data: any; // e.g. { id, instruction } or { id, nodeId } or { id, resultDoc }
  index?: number; // The log index for this entry
}

/*****************************************************************************
 * 2) Raft Node Callbacks Interface
 *    - chooseToWork: whether this node is suitable for the given instruction
 *    - performWork: actually perform the work (once assigned)
 *****************************************************************************/
export interface IRaftNodeCallbacks {
  /**
   * Decide if this node is suitable for the given WorkInstruction.
   * Return true if the node *wants* to claim the work.
   */
  chooseToWork(node: RaftNode, instruction: WorkInstruction): boolean;

  /**
   * Perform the actual work. This is called once the node is assigned a task.
   * This should ultimately call `completeWork(id, result)` on the Raft node.
   */
  performWork(
    node: RaftNode,
    taskId: string,
    instruction: WorkInstruction
  ): Promise<void>;
}

/*****************************************************************************
 * 3) Raft Node Class
 *    - In-memory, simplified Raft logic:
 *      -> Leader election
 *      -> Log replication
 *    - Task queue as the "state machine"
 *    - Integration with Iroh for p2p
 *****************************************************************************/
export class RaftNode {
  private iroh: Iroh;
  private author: AuthorId;

  // Node identity for Raft
  private nodeId: string; // e.g. a string version of (await iroh.net.nodeId())
  private currentTerm = 0;
  private votedFor: string | null = null;

  // Raft log
  private log: IRaftCommand[] = [];
  private commitIndex = 0; // Highest log index known to be committed
  private lastApplied = 0; // Highest log index applied to state machine

  // Cluster info
  private peerIds: string[] = []; // Node IDs for other Raft nodes
  private leaderId: string | null = null;

  // Timers
  private electionTimeoutHandle: any = null;
  private heartbeatIntervalHandle: any = null;

  // State machine: tasks by ID
  private tasks: Map<string, TaskRecord> = new Map();

  // Callbacks
  private callbacks: IRaftNodeCallbacks;

  constructor(iroh: Iroh, callbacks: IRaftNodeCallbacks) {
    this.iroh = iroh;
    this.callbacks = callbacks;
    this.nodeId = ""; // assigned in init()
    this.author = null as any; // assigned in init()
  }

  /*************************************************************************
   * Initialization
   *************************************************************************/
  public async init(peerAddrs: NodeAddr[]): Promise<void> {
    // Identify this node
    const rawNodeId = await this.iroh.net.nodeId();
    this.nodeId = rawNodeId.toString();
    this.author = await this.iroh.authors.default();

    // Connect to peers
    for (const addr of peerAddrs) {
      await this.iroh.net.addNodeAddr(addr);
    }

    // We also need to discover each peer's nodeId
    // In a real system, you'd have a known list of NodeIds or run a discovery handshake.
    // For simplicity, assume we already know them or fetch them from config.
    // For example, you might do:
    //   this.peerIds = [...some known list...];
    // or continue with an external “bootstrap” approach.

    // Start as a follower
    this.becomeFollower();

    // Kick off the election timer
    this.resetElectionTimer();

    // (Optional) Register message handlers for RequestVote, AppendEntries, etc.
    // In a real codebase, you'd do something like:
    //   this.iroh.gossip.subscribe(...)
    //   or direct p2p messages with iroh.net.send(...) / iroh.net.handle(...)
    //   depending on how you want to route Raft messages.
    //
    // For brevity, we will just outline the placeholders below:

    // E.g.:
    // this.iroh.net.onMessage(async (msg) => this.handleMessage(msg));
  }

  /*************************************************************************
   * Public API: Creating and Completing Work
   *************************************************************************/

  /**
   * Create work in the cluster. Returns the GUID of the newly created task.
   * Waits for the command to commit, then returns a DocTicket for reading results.
   */
  public async createWork(
    instruction: WorkInstruction
  ): Promise<{ id: string; resultDocTicket: string }> {
    // We generate a task ID (GUID)
    const taskId = randomUUID();

    // We also create a doc for the result. The doc ticket is stored in the log.
    const doc = await this.iroh.docs.create();
    const docTicket = await doc.share(
      ShareMode.Write,
      AddrInfoOptions.RelayAndAddresses
    );
    const docTicketStr = docTicket.toString();

    // Propose the "CREATE" command to the Raft log
    const cmd: IRaftCommand = {
      term: this.currentTerm,
      type: "CREATE",
      data: {
        id: taskId,
        instruction,
        resultDoc: docTicketStr,
      },
    };

    const logIndex = await this.replicateCommand(cmd);
    // Wait until the logIndex is committed
    await this.waitForCommit(logIndex);

    return { id: taskId, resultDocTicket: docTicketStr };
  }

  /**
   * Once the node is assigned a task and has performed it, it calls completeWork()
   * with the final output. The output itself is stored in the doc behind `resultDoc`.
   * We just replicate a "COMPLETE" command to update the cluster state that the
   * task is done.
   */
  public async completeWork(taskId: string): Promise<void> {
    // Propose the "COMPLETE" command
    // The actual results are stored in the doc. The doc ticket is already known from the CREATE step.
    const cmd: IRaftCommand = {
      term: this.currentTerm,
      type: "COMPLETE",
      data: { id: taskId },
    };

    const logIndex = await this.replicateCommand(cmd);
    await this.waitForCommit(logIndex);
  }

  /*************************************************************************
   * 4) Raft Core Logic (Simplified)
   *************************************************************************/

  /**
   * replicateCommand(): Called by createWork(), completeWork(), or any
   * other commands. If we are the leader, we append the command to our log
   * and replicate to followers. If we are not the leader, we forward to the
   * leader. (Simplified: no redirect in code snippet, but you'd do that in production.)
   */
  private async replicateCommand(cmd: IRaftCommand): Promise<number> {
    if (this.leaderId !== this.nodeId) {
      // In real production code, you’d forward the request to the leader.
      // For brevity, just append locally if we happen to be the leader, else throw:
      throw new Error(
        `Node ${this.nodeId} is not leader. Leader = ${this.leaderId}`
      );
    }

    // Append to local log
    cmd.index = this.log.length;
    this.log.push(cmd);

    // "Send" to all followers via Iroh messages (placeholder):
    //   for (const peerId of this.peerIds) {
    //     if (peerId !== this.nodeId) {
    //       await this.sendAppendEntries(peerId, ...);
    //     }
    //   }
    // In real code, you'd wait for a majority ack. We'll just assume success.

    // For simplicity, mark commitIndex = length of log - 1 (immediate commit).
    // In real Raft, you must wait for majority acknowledges.
    this.commitIndex = this.log.length - 1;
    this.applyLogEntries();

    return cmd.index!;
  }

  /**
   * Wait until the given log index is committed.
   * In real code, you'd watch for commitIndex >= logIndex.
   * Here, we do it synchronously in replicateCommand() for brevity.
   */
  private async waitForCommit(logIndex: number): Promise<void> {
    // No-op in this simplified version.
    // Could implement an event-based approach: e.g. an event emitter that fires when commitIndex changes.
    return;
  }

  /**
   * applyLogEntries():
   * Apply all newly committed log entries to the local state machine (the tasks map).
   */
  private applyLogEntries() {
    while (this.lastApplied < this.commitIndex + 1) {
      const entry = this.log[this.lastApplied];
      this.applyEntry(entry);
      this.lastApplied++;
    }
  }

  /**
   * applyEntry(): apply a single log entry to the tasks state machine.
   */
  private applyEntry(entry: IRaftCommand) {
    const { type, data } = entry;
    switch (type) {
      case "CREATE":
        {
          const { id, instruction, resultDoc } = data;
          this.tasks.set(id, {
            id,
            instruction,
            assignedTo: undefined,
            completed: false,
            resultDoc,
          });
        }
        break;

      case "ASSIGN":
        {
          const { id, nodeId } = data;
          const task = this.tasks.get(id);
          if (task && !task.assignedTo && !task.completed) {
            task.assignedTo = nodeId;
            // If this node is the one assigned, we trigger performWork callback
            if (nodeId === this.nodeId) {
              void this.callbacks
                .performWork(this, task.id, task.instruction)
                .catch((err) => console.error("performWork failed:", err));
            }
          }
        }
        break;

      case "COMPLETE":
        {
          const { id } = data;
          const task = this.tasks.get(id);
          if (task) {
            task.completed = true;
          }
        }
        break;
    }
  }

  /*************************************************************************
   * 5) Leader Election (Placeholder)
   *************************************************************************/

  private becomeFollower() {
    // Cancel heartbeat timer if any
    if (this.heartbeatIntervalHandle) {
      clearInterval(this.heartbeatIntervalHandle);
      this.heartbeatIntervalHandle = null;
    }
    // We remain a follower
  }

  private becomeLeader() {
    this.leaderId = this.nodeId;
    // Send heartbeats
    this.heartbeatIntervalHandle = setInterval(() => {
      // sendAppendEntries() to followers
    }, 1500);
  }

  /**
   * We do a random election timeout. If no heartbeat from a leader, we become candidate.
   */
  private resetElectionTimer() {
    if (this.electionTimeoutHandle) {
      clearTimeout(this.electionTimeoutHandle);
    }

    const timeout = 1500 + Math.round(Math.random() * 1500);
    this.electionTimeoutHandle = setTimeout(() => {
      // becomeCandidate
      // increment currentTerm
      // requestVote from peers
      // if we get majority, becomeLeader()
      this.currentTerm++;
      this.becomeLeader(); // In a real system, only if we get majority
    }, timeout);
  }

  /*************************************************************************
   * 6) Periodic Checking for Assignments
   *    The LEADER can attempt to assign tasks to nodes that are willing to do them.
   *************************************************************************/

  public startAssignmentLoop(intervalMs = 2000) {
    // If you only want the leader to do assignment, check if (this.nodeId === this.leaderId).
    setInterval(() => {
      if (this.nodeId !== this.leaderId) {
        return; // only leader tries to assign tasks
      }
      for (const task of this.tasks.values()) {
        if (!task.assignedTo && !task.completed) {
          // Check each peer (including self) to see if they'd like to do it
          for (const peerId of [this.nodeId, ...this.peerIds]) {
            // In real code, you'd have a callback from that node or a broadcast
            // For simplicity, let’s just see if *this node* is willing:
            if (peerId === this.nodeId) {
              const wantsIt = this.callbacks.chooseToWork(
                this,
                task.instruction
              );
              if (wantsIt) {
                this.assignTask(task.id, peerId);
                break;
              }
            }
            // or if we had a known mapping of node -> chooseToWork calls
          }
        }
      }
    }, intervalMs);
  }

  private async assignTask(taskId: string, toNodeId: string) {
    const cmd: IRaftCommand = {
      term: this.currentTerm,
      type: "ASSIGN",
      data: { id: taskId, nodeId: toNodeId },
    };
    const logIndex = await this.replicateCommand(cmd);
    await this.waitForCommit(logIndex);
  }

  /*************************************************************************
   * 7) Shutdown
   *************************************************************************/
  public async shutdown() {
    if (this.electionTimeoutHandle) {
      clearTimeout(this.electionTimeoutHandle);
    }
    if (this.heartbeatIntervalHandle) {
      clearInterval(this.heartbeatIntervalHandle);
    }
    await this.iroh.node.shutdown();
  }
}

class MyCallbacks implements IRaftNodeCallbacks {
  constructor(private capabilityTag: string) {}

  chooseToWork(node: RaftNode, instruction: WorkInstruction): boolean {
    // Simple rule: pick tasks that contain the node’s capabilityTag
    return instruction.includes(this.capabilityTag);
  }

  async performWork(
    node: RaftNode,
    taskId: string,
    instruction: WorkInstruction
  ): Promise<void> {
    console.log(
      `Node ${node["nodeId"]} performing task ${taskId} => ${instruction}`
    );
    // Simulate some work
    await new Promise((res) => setTimeout(res, 2000));

    // Write result to doc + complete
    const resultString = `Done by node ${node["nodeId"]}: ${instruction}`;
    const task = (node as any).tasks.get(taskId);
    const docTicketStr = task?.resultDoc;
    if (docTicketStr) {
      const docTicket = DocTicket.fromString(docTicketStr);
      const doc = await (node as any).iroh.docs.join(docTicket);
      if (doc) {
        const key = Array.from(Buffer.from(taskId));
        const value = Array.from(Buffer.from(resultString));
        await doc.setBytes((node as any).author, key, value);
      }
    }

    await node.completeWork(taskId);
  }
}

async function main() {
  // 1) Create an array of Iroh instances (one per node).
  //    In real usage, you'd have them on different machines, each with known addresses.
  const iroh1 = await Iroh.memory({ enableDocs: true });
  const iroh2 = await Iroh.memory({ enableDocs: true });
  const iroh3 = await Iroh.memory({ enableDocs: true });

  // 2) Get addresses
  const addr1 = await iroh1.net.nodeAddr();
  const addr2 = await iroh2.net.nodeAddr();
  const addr3 = await iroh3.net.nodeAddr();

  // 3) Create RaftNodes with different callbacks
  const node1 = new RaftNode(iroh1, new MyCallbacks("alpha"));
  const node2 = new RaftNode(iroh2, new MyCallbacks("beta"));
  const node3 = new RaftNode(iroh3, new MyCallbacks("alpha"));

  // 4) Initialize them, presumably with each other’s addresses
  await node1.init([addr2, addr3]);
  await node2.init([addr1, addr3]);
  await node3.init([addr1, addr2]);

  // 5) Start assignment loops
  node1.startAssignmentLoop();
  node2.startAssignmentLoop();
  node3.startAssignmentLoop();

  // 6) Wait a bit, then let node1 create some work
  setTimeout(async () => {
    console.log("\nCreating tasks on Node 1...");
    const { id, resultDocTicket } = await node1.createWork(
      "Process data with alpha?"
    );
    console.log(`Created task = ${id}, docTicket = ${resultDocTicket}`);

    // Wait for completion
    setTimeout(async () => {
      // Join the doc to see the result
      const docTicket = DocTicket.fromString(resultDocTicket);
      const doc = await iroh1.docs.join(docTicket);
      if (doc) {
        const resultKey = Array.from(Buffer.from(id));
        const result = await doc.getOne(Query.keyExact(resultKey));
        if (result) {
          const resultData = await iroh1.blobs.readToBytes(result.hash);
          console.log("Result from doc:", Buffer.from(resultData).toString());
        }
      }
    }, 8000);
  }, 3000);

  // Graceful shutdown on Ctrl-C
  process.on("SIGINT", async () => {
    console.log("Shutting down...");
    await node1.shutdown();
    await node2.shutdown();
    await node3.shutdown();
    process.exit(0);
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
