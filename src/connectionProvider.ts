import { BiStream, Connection, Endpoint, NodeAddr } from "@number0/iroh";

type ActiveConnection = {
  address: NodeAddr;
  connection: Promise<Connection>;
  initialized: boolean;
  lastEvent: Date;
  activeStreams: number;
};

export type ConnectionProviderOptions = {
  keepAliveTime?: number;
};

export class ConnectionProvider {
  constructor(
    public readonly endpoint: Endpoint,
    public readonly protocol: string,
    private readonly opts?: ConnectionProviderOptions
  ) {}

  connections = new Map<string, ActiveConnection>();

  get keepAliveTime() {
    return this.opts?.keepAliveTime ?? 5000; // Default to 5 seconds
  }

  private tryCleanupConnection(address: string) {
    const connection = this.connections.get(address);
    if (!connection) {
      return;
    }

    if (!connection.initialized) {
      return;
    }

    if (connection.activeStreams > 0) {
      return;
    }

    if (Date.now() - connection.lastEvent.getTime() > this.keepAliveTime) {
      this.connections.delete(address);
    }
  }

  private makeGcTask() {
    setTimeout(() => {
      for (const [address, connection] of this.connections.entries()) {
        this.tryCleanupConnection(address);
      }
    }, this.keepAliveTime + 50);
  }

  private spawnConnection(address: NodeAddr) {
    const connection = this.endpoint.connect(
      address,
      Buffer.from(this.protocol)
    );

    const connectionData: ActiveConnection = {
      address,
      connection,
      initialized: false,
      lastEvent: new Date(),
      activeStreams: 0,
    };

    this.connections.set(address.nodeId, connectionData);

    connection.then((conn) => {
      this.markConnectionInitialized(address);
      this.makeGcTask();
    });

    return connectionData;
  }

  private markConnectionInitialized(address: NodeAddr) {
    const connection = this.connections.get(address.nodeId);
    if (!connection) {
      return;
    }

    connection.initialized = true;
  }

  private openStream(address: NodeAddr) {
    let connectionData = this.connections.get(address.nodeId);
    if (!connectionData) {
      connectionData = this.spawnConnection(address);
    }

    connectionData.lastEvent = new Date();
    connectionData.activeStreams++;
    return connectionData.connection
      .then((conn) => conn.openBi())
      .catch((err) => {
        this.connections.delete(address.nodeId);
        throw err;
      });
  }

  private onCloseStream(address: NodeAddr) {
    const connection = this.connections.get(address.nodeId);
    if (!connection) {
      return;
    }

    connection.lastEvent = new Date();
    connection.activeStreams--;
    this.makeGcTask();
  }

  async withConnection<T>(address: NodeAddr, fn: (bi: BiStream) => T) {
    const bi = await this.openStream(address);

    try {
      const result = await fn(bi);
      return result;
    } finally {
      this.onCloseStream(address);
    }
  }
}
