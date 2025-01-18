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
import { protocolName } from "./helpers";
import { makeTrpcIrohClient } from "./client";

// 1) Create a basic tRPC Router
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

// 3) The main entry point
async function main() {
  console.log("[Main] Starting iroh-trpc server...");

  // 5) Create two nodes: Node1 (server) & Node2 (client)
  console.log("[Main] Creating nodes...");
  const node1 = await Iroh.memory({
    protocols: makeTrpcIrohProtocols({ router: appRouter }),
  });
  const node2 = await Iroh.memory({
    protocols: makeTrpcIrohProtocols({ router: appRouter }),
  });

  const node1Addr = await node1.net.nodeAddr();
  console.log("[Main] Node1 ID:", node1Addr.nodeId.toString());

  const endpoint = node2.node.endpoint();

  const client = makeTrpcIrohClient<AppRouter>(endpoint);
  const node1Client = client.node(node1Addr);

  console.log("[Main] Testing call");
  //   const result = await node1Client.hello.query();
  const promise1 = new Promise((resolve) => {
    const sub = node1Client.countdown.subscribe(5, {
      onData(data) {
        console.log("[Main] Received data:", data);
        // if (data === 5) {
        //   sub.unsubscribe();
        //   resolve(null);
        // }
      },
      onError(err) {
        console.error("[Main] Subscription error:", err);
      },
      onComplete() {
        console.log("[Main] Subscription complete");
        resolve(null);
      },
    });
  });

  const promise2 = new Promise((resolve) => {
    const sub = node1Client.countdown.subscribe(9, {
      onData(data) {
        console.log("[Main] Received data:", data);
      },
      onError(err) {
        console.error("[Main] Subscription error:", err);
      },
      onComplete() {
        console.log("[Main] Subscription complete");
        resolve(null);
      },
    });
  });

  await Promise.all([promise1, promise2]);

  await new Promise((resolve) => setTimeout(resolve, 10000));

  // Make another call
  console.log("[Main] Testing call");
  const result = await node1Client.hello.query();
  console.log("[Main] Hello result:", result);

  console.log("[Main] Shutting down nodes...");
  await node2.node.shutdown();
  await node1.node.shutdown();
  console.log("[Main] Clean shutdown complete");
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});
