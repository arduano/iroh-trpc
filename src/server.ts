import { Endpoint, ProtocolHandler, Connection, BiStream } from "@number0/iroh";
import { AnyTRPCRouter, callTRPCProcedure, TRPCError } from "@trpc/server";
import {
  isObservable,
  observableToAsyncIterable,
} from "@trpc/server/observable";
import { isAsyncIterable } from "@trpc/server/unstable-core-do-not-import";
import {
  CancelRequest,
  protocolName,
  readMessage,
  RequestType,
  ResponseType,
  writeEndStream,
  writeMessage,
} from "./helpers";

export function makeTrpcIrohProtocols<AppRouter extends AnyTRPCRouter>(args: {
  router: AppRouter;
}) {
  const { router: appRouter } = args;

  const protocols = {
    [protocolName]: (err: Error | null, ep: Endpoint): ProtocolHandler => ({
      accept: async (err, connecting) => {
        // We are the "server" side
        if (err) {
          console.error("[tRPC Iroh] Accept error:", err);
          return;
        }

        let conn: Connection;
        try {
          conn = await connecting.connect();
        } catch (err) {
          console.error("[tRPC Iroh] Connection error:", err);
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
              .catch((err) => {
              });

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
                  "[tRPC Iroh] Invalid iterable result for non-subscription"
                );
                throw new TRPCError({
                  code: "BAD_REQUEST",
                  message:
                    "Cannot return an async iterable in a non-subscription call",
                });
              }

              await writeMessage<ResponseType>(bi.send, { data: result });
            } else {
              if (!isIterableResult) {
                console.error(
                  "[tRPC Iroh] Invalid non-iterable result for subscription"
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
                  await writeMessage<ResponseType>(bi.send, { data: item });
                } catch (e) {
                  return;
                }
              }
            }

            await writeEndStream(bi.send);
            await conn.closed();
          } catch (e) {
            console.error(
              "[tRPC Iroh] Error handling iroh-trpc/v1 stream request:",
              e
            );
          }
        };

        try {
          while (true) {
            const bi = await conn.acceptBi();
            void handleStream(bi).catch((e) => {
            });
          }
        } catch (e) {
        }
      },
      shutdown: (err: Error | null) => {
        if (err) {
          console.error("[tRPC Iroh] Shutdown error:", err);
        }
      },
    }),
  };

  return protocols;
}
