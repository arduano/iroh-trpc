import { AddrInfoOptions, AuthorId, BlobDownloadOptions, BlobFormat, DocTicket, Iroh, Query, ShareMode } from "@number0/iroh";

interface NodeData {
  iroh: Iroh;
  id: string;
  number: number;
  author: AuthorId;
}

async function createNode(number: number): Promise<NodeData> {
  const node = await Iroh.memory({ enableDocs: true });
  const id = (await node.net.nodeId()).toString();
  const author = await node.authors.default();

  return { iroh: node, id, number, author };
}

interface DocData {
  id: string;
  ticket: string;
  hash?: string;
}

async function createDocOnNode(
  node: NodeData,
  content: string
): Promise<DocData> {
  console.log(`Node ${node.number}: Creating new document`);

  // Create a new document
  const doc = await node.iroh.docs.create();
  const docId = doc.id();

  // Create content
  const key = Array.from(Buffer.from("content"));
  const value = Array.from(Buffer.from(content));
  const hash = await doc.setBytes(node.author, key, value);

  console.log(
    `Node ${node.number}: Created document ${docId} with content "${content}"`
  );

  // Share the document
  const ticket = await doc.share(ShareMode.Write, AddrInfoOptions.RelayAndAddresses);

  return {
    id: docId,
    ticket: ticket.toString(),
    hash: hash.toString(),
  };
}

async function joinDoc(
  node: NodeData,
  ticket: string,
  onSync?: () => void
): Promise<void> {
  console.log(`Node ${node.number}: Joining document`);

  // Parse the ticket
  const docTicket = DocTicket.fromString(ticket);

  // Join and subscribe to the document
  await node.iroh.docs.joinAndSubscribe(docTicket, (error, event) => {
    if (error) {
      console.error(`Node ${node.number} error:`, error);
      return;
    }

    if (event.syncFinished) {
      console.log(`Node ${node.number}: Sync finished`);
      if (onSync) onSync();
    }
  });
}

async function addContentToDoc(
  node: NodeData,
  ticket: string,
  content: string
): Promise<void> {
  console.log(`Node ${node.number}: Adding content "${content}"`);

  const docTicket = DocTicket.fromString(ticket);
  const doc = await node.iroh.docs.join(docTicket);

  if (!doc) {
    console.error(`Node ${node.number}: Document not found`);
    return;
  }

  const key = Array.from(Buffer.from(`content-${Date.now()}`));
  const value = Array.from(Buffer.from(content));
  const hash = await doc.setBytes(node.author, key, value);

  console.log(`Node ${node.number}: Added content with hash ${hash}`);
}

async function startDocNetwork(numNodes: number) {
  // Create nodes
  const nodes: NodeData[] = [];
  for (let i = 1; i <= numNodes; i++) {
    const node = await createNode(i);
    nodes.push(node);
    console.log(`Created Node ${node.number} (${node.id})`);
  }

  // Node 1 creates a document
  const doc = await createDocOnNode(nodes[0], "Initial content from Node 1");

  // All other nodes join the document
  await Promise.all(
    nodes.slice(1).map((node) =>
      joinDoc(node, doc.ticket, async () => {
        // After sync, each node adds their own content
        await addContentToDoc(
          node,
          doc.ticket,
          `Hello from Node ${node.number}!`
        );
      })
    )
  );

  // Set up cleanup
  process.on("SIGINT", async () => {
    console.log("\nShutting down nodes...");
    await Promise.all(nodes.map((node) => node.iroh.node.shutdown()));
    process.exit(0);
  });

  console.log(
    `\nNetwork running with ${numNodes} nodes. Press Ctrl+C to stop.`
  );
  console.log("Document ticket:", doc.ticket);

  // Periodically add content from random nodes
  setInterval(async () => {
    const randomNode = nodes[Math.floor(Math.random() * nodes.length)];
    await addContentToDoc(
      randomNode,
      doc.ticket,
      `Random update from Node ${randomNode.number}`
    );
  }, 5000);
}

async function main() {
  await startDocNetwork(4);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
