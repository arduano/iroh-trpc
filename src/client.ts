import { Connection, BiStream, Endpoint, NodeAddr } from "@number0/iroh";
import { TRPCLink, TRPCClientError, createTRPCClient } from "@trpc/client";
import { observable } from "@trpc/server/observable";
import {
  writeCancelMessage,
  writeEndStream,
  RequestType,
  writeMessage,
  readMessage,
  protocolName,
  ResponseType,
} from "./helpers";
import { AnyTRPCRouter } from "@trpc/server";
import {
  ConnectionProvider,
  ConnectionProviderOptions,
} from "./connectionProvider";

export function makeTrpcIrohClientForAddress<AppRouter extends AnyTRPCRouter>(
  provider: ConnectionProvider,
  address: NodeAddr
) {
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
              console.error("[tRPC Iroh] Error sending cancel event:", err);
            }
          }
        };

        const cancel = async () => {
          if (!cancelled && !completed) {
            cancelled = true;
            complete();
            await sendCancelEvent().catch((err) => {
              console.error("[tRPC Iroh] Error in cancel:", err);
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

            await provider.withConnection(address, async (bi) => {
              try {
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

                  const response = await readMessage<ResponseType>(bi.recv);
                  if (!response) {
                    break;
                  }

                  if (cancelled) {
                    return;
                  }

                  observer.next({
                    result: {
                      type: "data",
                      data: response.data,
                    },
                    context: op.context,
                  });
                }

                complete();
                observer.complete();
              } catch (error) {
                console.error("[tRPC Iroh] Request error:", error);
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
                  }
                }
              }
            });
          } catch (err) {
            console.error("[tRPC Iroh] Connection error:", err);
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

  return trpcClient;
}

export function makeTrpcIrohClient<AppRouter extends AnyTRPCRouter>(
  endpoint: Endpoint,
  options?: ConnectionProviderOptions
) {
  const provider = new ConnectionProvider(endpoint, protocolName, options);
  return {
    node: (address: NodeAddr) =>
      makeTrpcIrohClientForAddress<AppRouter>(provider, address),
  };
}
