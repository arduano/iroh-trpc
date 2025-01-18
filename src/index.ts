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
  // Create a custom protocol name
  const protocolName = Buffer.from("iroh-trpc/v1");

  // 4) Implement the server (Node1) side protocol handler
  const protocols = {
    "iroh-trpc/v1": (err: Error | null, ep: Endpoint): ProtocolHandler => ({
      accept: async (err, connecting) => {
        // We are the "server" side (Node1).
        if (err) {
          console.error("[Server] Accept error:", err);
          return;
        }

        let conn: Connection;
        try {
          conn = await connecting.connect();
        } catch (err) {
          console.error("[Server] Connection error:", err);
          throw err;
        }

        const handleStream = async (bi: BiStream) => {
          try {
            // Read request
            const request = await readMessage<RequestType>(bi.recv);
            if (!request) {
              return;
            }

            const controller = new AbortController();

            // Spawn cancel listener
            void readMessage<CancelRequest>(bi.recv)
              .then((req) => {
                if (req) {
                  controller.abort();
                }
              })
              .catch((err) => {});

            // Handle the tRPC request
            const result = await callTRPCProcedure({
              ctx: {},
              path: request.path,
              input: request.input,
              procedures: appRouter._def.procedures,
              getRawInput: async () => request.input,
              type: request.type,
              signal: controller.signal,
            });

            if (controller.signal.aborted) {
              return;
            }

            const isIterableResult =
              isAsyncIterable(result) || isObservable(result);

            if (controller.signal.aborted) {
              await writeEndStream(bi.send);
              return;
            }

            if (request.type !== "subscription") {
              if (isIterableResult) {
                console.error(
                  "[Server] Invalid iterable result for non-subscription"
                );
                throw new TRPCError({
                  code: "BAD_REQUEST",
                  message:
                    "Cannot return an async iterable in a non-subscription call",
                });
              }

              await writeMessage(bi.send, result);
            } else {
              if (!isIterableResult) {
                console.error(
                  "[Server] Invalid non-iterable result for subscription"
                );
                throw new TRPCError({
                  code: "BAD_REQUEST",
                  message:
                    "Cannot return a non-async iterable in a subscription call",
                });
              }

              const iterable = isObservable(result)
                ? observableToAsyncIterable(result, controller.signal)
                : result;
              for await (const item of iterable) {
                if (controller.signal.aborted) {
                  break;
                }

                try {
                  await writeMessage(bi.send, item);
                } catch (e) {
                  // Connection lost
                  console.error("[Server] Connection lost:", e);
                  return;
                }
              }
            }

            await writeEndStream(bi.send);
            await conn.closed();
          } catch (e) {
            console.error(
              "[Server] Error handling iroh-trpc/v1 stream request:",
              e
            );
          }
        };

        try {
          while (true) {
            const bi = await conn.acceptBi();
            void handleStream(bi).catch(() => {});
          }
        } catch (e) {
          // Connection closed
        }
      },
      shutdown: (err: Error | null) => {
        if (err) console.error("[Server] Shutdown error:", err);
        else console.log("[Server] Clean shutdown");
      },
    }),
  };

  // 5) Create two nodes: Node1 (server) & Node2 (client)
  console.log("[Main] Creating nodes...");
  const node1 = await Iroh.memory({ protocols });
  const node2 = await Iroh.memory({ protocols });

  // Get Node1's address
  const node1Addr = await node1.net.nodeAddr();
  console.log("[Main] Node1 ID:", node1Addr.nodeId.toString());

  // 6) Create the tRPC client on Node2, with a custom iroh link
  //    We will open a new connection+stream for each procedure call.
  const endpoint = node2.node.endpoint();

  const conn = await endpoint.connect(node1Addr, protocolName);

  const irohTRPCLink: TRPCLink<any> = () => {
    return ({ op }) => {
      return observable((observer) => {
        let cancelled = false;
        let completed = false;
        let sentCancelEvent = false;
        let biStream: BiStream | undefined;

        const sendCancelEvent = async () => {
          if (!sentCancelEvent && biStream) {
            sentCancelEvent = true;
            try {
              await writeCancelMessage(biStream.send);
              await writeEndStream(biStream.send);
            } catch (err) {
              console.error("[Client] Error sending cancel event:", err);
            }
          }
        };

        const cancel = async () => {
          if (!cancelled && !completed) {
            cancelled = true;
            complete();
            await sendCancelEvent().catch((err) => {
              console.error("[Client] Error in cancel:", err);
            });
          }
        };

        const complete = async () => {
          if (!completed) {
            completed = true;
          }
        };

        op.signal?.addEventListener("abort", cancel);

        const execute = async () => {
          try {
            // Construct request object
            const request: RequestType = {
              id: op.id,
              type: op.type,
              path: op.path,
              input: op.input,
            };

            if (cancelled) {
              observer.complete();
              return;
            }

            // Connect to Node1 using the iroh protocol
            const bi = await conn.openBi();
            biStream = bi;

            if (cancelled) {
              observer.complete();
              await sendCancelEvent();
              complete();
              return;
            }

            // Send request
            await writeMessage(bi.send, request);

            // While there's responses, read them
            while (true) {
              if (cancelled) {
                return;
              }

              const response = await readMessage(bi.recv);
              if (!response) {
                break;
              }

              if (cancelled) {
                return;
              }

              observer.next({
                result: {
                  type: "data",
                  data: response,
                },
                context: op.context,
              });
            }

            complete();
            observer.complete();
          } catch (error) {
            console.error("[Client] Request error:", error);
            if (error instanceof TRPCClientError) {
              observer.error(error);
            } else {
              observer.error(
                new TRPCClientError((error as any).message, {
                  cause: error as any,
                })
              );
            }
          } finally {
            // Clean up on completion
            if (biStream) {
              try {
                await writeEndStream(biStream.send);
              } catch (error) {
                console.error("[Client] Error during biStream cleanup:", error);
              }
            }
          }
        };

        execute();

        return cancel;
      });
    };
  };

  const trpcClient = createTRPCClient<AppRouter>({
    links: [irohTRPCLink],
  });

  // 7) Test multiple tRPC calls from Node2 to Node1
  const testCalls = async () => {
    // Run each test sequentially
    console.log("\n[Test] Starting regular query");
    try {
      const queryResponse = await trpcClient.hello.query();
      console.log("[Test] Query response:", queryResponse);
    } catch (err) {
      console.error("[Test] Query error:", err);
    }

    await new Promise((resolve) => setTimeout(resolve, 2000));

    console.log("\n[Test] Starting infinite counter subscription");
    try {
      await new Promise<void>((resolve, reject) => {
        const controller = new AbortController();
        let completed = false;

        const cleanup = () => {
          if (!completed) {
            completed = true;
            controller.abort();
            resolve();
          }
        };

        // Timeout safety
        const timeout = setTimeout(() => {
          console.error("[Test] Infinite counter timeout");
          cleanup();
        }, 10000);

        trpcClient.infiniteCounter.subscribe(undefined, {
          onData: (count) => {
            console.log("[Test] Received count:", count);
          },
          onComplete: () => {
            clearTimeout(timeout);
            console.log("[Test] Infinite counter completed");
            cleanup();
          },
          onError: (err) => {
            clearTimeout(timeout);
            console.error("[Test] Infinite counter error:", err);
            cleanup();
          },
          signal: controller.signal,
        });

        // Cancel after 5 seconds
        setTimeout(() => {
          console.log("[Test] Cancelling infinite counter");
          cleanup();
        }, 5000);
      });
    } catch (err) {
      console.error("[Test] Infinite counter error:", err);
    }

    await new Promise((resolve) => setTimeout(resolve, 2000));

    console.log("\n[Test] Starting countdown subscription");
    try {
      await new Promise<void>((resolve, reject) => {
        let completed = false;

        const cleanup = () => {
          if (!completed) {
            completed = true;
            resolve();
          }
        };

        // Timeout safety
        const timeout = setTimeout(() => {
          console.error("[Test] Countdown timeout");
          cleanup();
        }, 10000);

        trpcClient.countdown.subscribe(3, {
          onData: (count) => {
            console.log("[Test] Received countdown:", count);
          },
          onComplete: () => {
            clearTimeout(timeout);
            console.log("[Test] Countdown completed");
            cleanup();
          },
          onError: (err) => {
            clearTimeout(timeout);
            console.error("[Test] Countdown error:", err);
            cleanup();
          },
        });
      });
    } catch (err) {
      console.error("[Test] Countdown error:", err);
    }

    console.log("\n[Test] All tests completed");
  };

  await testCalls();

  // 8) Cleanup
  console.log("[Main] Shutting down nodes...");
  await node2.node.shutdown();
  await node1.node.shutdown();
  console.log("[Main] Clean shutdown complete");
}

// Stream helpers
const MESSAGE_PREFIX = Buffer.from("JSON");

async function writeMessage(stream: SendStream, data: unknown) {
  const serialized = superjson.stringify(data);
  const serializedBuf = Buffer.from(serialized);
  const lengthBuf = Buffer.alloc(4);
  lengthBuf.writeUInt32LE(serializedBuf.length, 0);

  await stream.writeAll(MESSAGE_PREFIX);
  await stream.writeAll(lengthBuf);
  await stream.writeAll(serializedBuf);
}

async function readMessage<T>(stream: RecvStream): Promise<T | null> {
  const prefixBuf = Buffer.alloc(4);
  await stream.readExact(prefixBuf);

  if (!prefixBuf.equals(MESSAGE_PREFIX)) {
    throw new Error(`Invalid message prefix: ${prefixBuf.toString()}`);
  }

  const lengthBuf = Buffer.alloc(4);
  await stream.readExact(lengthBuf);
  const length = lengthBuf.readUInt32LE(0);

  if (length === 0) {
    return null;
  }

  const dataBuf = Buffer.alloc(length);
  await stream.readExact(dataBuf);

  return superjson.parse(dataBuf.toString());
}

async function writeEndStream(stream: SendStream) {
  await stream.writeAll(MESSAGE_PREFIX);
  const lengthBuf = Buffer.alloc(4);
  lengthBuf.writeUInt32LE(0, 0);
  await stream.writeAll(lengthBuf);
}

async function writeCancelMessage(stream: SendStream) {
  await writeMessage(stream, { type: "cancel" });
}

main().catch((err) => {
  console.error("Error:", err);
  process.exit(1);
});

type RequestType = Omit<Operation, "context" | "signal">;
type CancelRequest = { type: "cancel" };
