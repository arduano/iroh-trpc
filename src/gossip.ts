import { Iroh, NodeAddr, Sender } from "@number0/iroh";

interface NodeData {
  iroh: Iroh;
  id: string;
  number: number;
  addr: NodeAddr;
  sink: Sender | null;
}

async function createNode(
  number: number,
  bootstrapNodes: NodeData[] = []
): Promise<NodeData> {
  const node = await Iroh.memory();
  const id = await node.net.nodeId();
  const addr = await node.net.nodeAddr();

  // Add bootstrap nodes to the network
  for (const bootstrapNode of bootstrapNodes) {
    await node.net.addNodeAddr(bootstrapNode.addr);
  }

  return { iroh: node, id: id.toString(), number, addr, sink: null };
}

function randomInt(min: number, max: number): number {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

async function setupNodeGossip(
  node: NodeData,
  topic: number[],
  otherNodes: NodeData[]
): Promise<() => Promise<void>> {
  // Get all other node IDs
  const otherNodeIds = otherNodes
    .filter((n) => n.id !== node.id)
    .map((n) => n.iroh.net.nodeId());

  // Make sure all nodes are connected to each other
  for (const otherNode of otherNodes) {
    if (otherNode.id !== node.id) {
      await node.iroh.net.addNodeAddr(otherNode.addr);
    }
  }

  // Subscribe to gossip
  const sink = await node.iroh.gossip.subscribe(
    topic,
    await Promise.all(otherNodeIds),
    (error, event) => {
      if (error) {
        console.error(`Node ${node.number} error:`, error);
        return;
      }

      if (event.received) {
        const message = Buffer.from(event.received.content).toString();
        const fromNode =
          otherNodes.find((n) => n.id === event.received!.deliveredFrom)
            ?.number || "unknown";
        console.log(
          `Node ${node.number} received: "${message}" from Node ${fromNode}`
        );
      }
    }
  );

  // Start random broadcasting
  const broadcast = async () => {
    try {
      const message = `Hello from Node ${node.number}! ${Date.now()}`;
      console.log(`Node ${node.number} broadcasting: "${message}"`);
      await sink.broadcast(Array.from(Buffer.from(message, "utf8")));
    } catch (error) {
      console.error(`Node ${node.number} broadcast error:`, error);
    }
  };

  // Start the broadcast loop with initial random delay
  setInterval(broadcast, randomInt(1000, 5000));

  // Return shutdown function
  return async () => {
    if (node.sink) await node.sink.close();
    await node.iroh.node.shutdown();
  };
}

async function startGossipNetwork(numNodes: number) {
  // Create nodes
  const nodes: NodeData[] = [];

  // Create first node
  const firstNode = await createNode(1);
  nodes.push(firstNode);

  // Create remaining nodes with first node as bootstrap
  for (let i = 2; i <= numNodes; i++) {
    const node = await createNode(i, [firstNode]);
    nodes.push(node);
  }

  // Create a topic (32 bytes, filled with zeros)
  const rawTopic = new Uint8Array(32);
  rawTopic.fill(0);
  const topic = Array.from(rawTopic);

  // Set up all nodes and collect their shutdown functions
  const shutdownFunctions = await Promise.all(
    nodes.map((node) => setupNodeGossip(node, topic, nodes))
  );

  // Handle cleanup on SIGINT
  process.on("SIGINT", async () => {
    console.log("\nShutting down nodes...");
    await Promise.all(shutdownFunctions.map((shutdown) => shutdown()));
    process.exit(0);
  });

  console.log(`Started ${numNodes} nodes. Press Ctrl+C to stop.`);
  console.log(
    "Nodes:",
    nodes.map((n) => `Node ${n.number} (${n.id})`).join("\n")
  );
}

async function main() {
  await startGossipNetwork(4);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
