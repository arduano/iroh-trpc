import { Command } from "commander";
import {
  BiStream,
  Connection,
  Endpoint,
  Iroh,
  ProtocolHandler,
  RecvStream,
  SendStream,
} from "@number0/iroh";
import {
  createTRPCClient,
  createTRPCProxyClient,
  Operation,
  TRPCClientError,
  TRPCLink,
} from "@trpc/client";
import { initTRPC, callTRPCProcedure } from "@trpc/server";
import {
  isObservable,
  observable,
  observableToAsyncIterable,
} from "@trpc/server/observable";
import {
  isAsyncIterable,
  TRPCError,
} from "@trpc/server/unstable-core-do-not-import";
import superjson from "superjson";
import { z } from "zod";
import { makeTrpcIrohProtocols } from "./server";
import { makeTrpcIrohClient } from "./client";

// Create a basic tRPC Router
const t = initTRPC.create();
const appRouter = t.router({
  hello: t.procedure.query(() => "world"),

  // Infinite counter that needs to be cancelled by client
  infiniteCounter: t.procedure.subscription(() => {
    return observable<number>((emit) => {
      let count = 0;
      const timer = setInterval(() => {
        emit.next(count++);
      }, 1000);

      return () => {
        clearInterval(timer);
      };
    });
  }),

  // Limited countdown that completes naturally
  countdown: t.procedure.input(z.number()).subscription(({ input: start }) => {
    return observable<number>((emit) => {
      let count = start;

      const timer = setInterval(() => {
        emit.next(count);

        if (count <= 0) {
          clearInterval(timer);
          emit.complete();
        }
        count--;
      }, 1000);

      return () => {
        clearInterval(timer);
      };
    });
  }),
});

type AppRouter = typeof appRouter;

async function startServer() {
  console.log("[Server] Starting iroh-trpc server...");

  const node = await Iroh.memory({
    protocols: makeTrpcIrohProtocols({ router: appRouter }),
  });

  const nodeAddr = await node.net.nodeAddr();

  // Create both address formats
  const basicAddr = serializeNodeAddr({
    nodeId: nodeAddr.nodeId,
  });

  const fullAddr = serializeNodeAddr({
    nodeId: nodeAddr.nodeId,
    relayUrl: nodeAddr.relayUrl,
    addresses: nodeAddr.addresses,
  });

  console.log("[Server] Node basic address:", basicAddr);
  console.log("[Server] Node full address:", fullAddr);
  console.log("[Server] Node ID:", nodeAddr.nodeId);

  // Keep the server running
  while (true) {
    await new Promise((resolve) => setTimeout(resolve, 1000));
  }
}

async function startClient(serverAddrStr: string) {
  console.log("[Client] Starting iroh-trpc client...");
  console.log("[Client] Connecting to server:", serverAddrStr);

  const node = await Iroh.memory({});
  const endpoint = node.node.endpoint();

  const client = makeTrpcIrohClient<AppRouter>(endpoint);

  // Parse the server address
  const serverAddr = deserializeNodeAddr(serverAddrStr);
  console.log("[Client] Parsed server address:", serverAddr);

  const serverClient = client.node(serverAddr);

  // Helper function to run a subscription
  const runSubscription = (count: number) =>
    new Promise<void>((resolve) => {
      console.log("[Client] Starting countdown from", count);
      const sub = serverClient.countdown.subscribe(count, {
        onData(data) {
          console.log("[Client] Countdown:", data);
        },
        onError(err) {
          console.error("[Client] Subscription error:", err);
          resolve();
        },
        onComplete() {
          console.log("[Client] Countdown complete");
          resolve();
        },
      });
    });

  // Test hello query
  console.log("[Client] Testing hello query...");
  const result = await serverClient.hello.query();
  console.log("[Client] Hello result:", result);

  // Test countdown subscription
  await runSubscription(5);

  // Cleanup
  await node.node.shutdown();
  console.log("[Client] Clean shutdown complete");
}

const program = new Command();

program
  .name("iroh-trpc")
  .description("tRPC over Iroh example application")
  .version("1.0.0");

program
  .command("server")
  .description("Start the tRPC server")
  .action(async () => {
    try {
      await startServer();
    } catch (err) {
      console.error("Server error:", err);
      process.exit(1);
    }
  });

program
  .command("client")
  .description("Start the tRPC client")
  .requiredOption("-s, --server <address>", "Server address (base64 encoded)")
  .action(async (options) => {
    try {
      await startClient(options.server);
    } catch (err) {
      console.error("Client error:", err);
      process.exit(1);
    }
  });

program.parse();

export type NodeAddressBasic = {
  type: "basic";
  nodeId: string;
};

export type NodeAddressFull = {
  type: "full";
  nodeId: string;
  relayUrl: string;
  addresses: string[];
};

export type SerializedNodeAddress = NodeAddressBasic | NodeAddressFull;

export function serializeNodeAddr(addr: {
  nodeId: string;
  relayUrl?: string;
  addresses?: string[];
}): string {
  const nodeId = addr.nodeId;

  if (!addr.relayUrl && !addr.addresses) {
    const basic: NodeAddressBasic = {
      type: "basic",
      nodeId,
    };
    return Buffer.from(JSON.stringify(basic)).toString("base64");
  }

  const full: NodeAddressFull = {
    type: "full",
    nodeId,
    relayUrl: addr.relayUrl || "",
    addresses: addr.addresses || [],
  };
  return Buffer.from(JSON.stringify(full)).toString("base64");
}

export function deserializeNodeAddr(serialized: string): SerializedNodeAddress {
  const json = Buffer.from(serialized, "base64").toString();
  const data = JSON.parse(json) as SerializedNodeAddress;

  if (data.type !== "basic" && data.type !== "full") {
    throw new Error("Invalid node address type");
  }

  return data;
}
