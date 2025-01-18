import { SendStream, RecvStream } from "@number0/iroh";
import { Operation } from "@trpc/client";
import superjson from "superjson";

// Stream helpers
const MESSAGE_PREFIX = Buffer.from("JSON");

export async function writeMessage<T>(stream: SendStream, data: T) {
  const serialized = superjson.stringify(data);
  const serializedBuf = Buffer.from(serialized);
  const lengthBuf = Buffer.alloc(4);
  lengthBuf.writeUInt32LE(serializedBuf.length, 0);

  await stream.writeAll(MESSAGE_PREFIX);
  await stream.writeAll(lengthBuf);
  await stream.writeAll(serializedBuf);
}

export async function readMessage<T>(stream: RecvStream): Promise<T | null> {
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

export async function writeEndStream(stream: SendStream) {
  await stream.writeAll(MESSAGE_PREFIX);
  const lengthBuf = Buffer.alloc(4);
  lengthBuf.writeUInt32LE(0, 0);
  await stream.writeAll(lengthBuf);
}

export async function writeCancelMessage(stream: SendStream) {
  await writeMessage(stream, { type: "cancel" });
}

export type RequestType = Omit<Operation, "context" | "signal">;
export type ResponseType = { data: unknown };
export type CancelRequest = { type: "cancel" };

export const protocolName = "iroh-trpc/v1";
