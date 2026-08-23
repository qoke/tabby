/* eslint-disable @typescript-eslint/no-unsafe-enum-comparison */
import { createServer, Server, Socket } from 'net'
import { Logger } from 'tabby-core'
import { ForwardedPortConfig, PortForwardType } from 'tabby-ssh'

import { ETProfileOptions } from '../api/interfaces'
import { ETPacketType, PORT_FORWARD_CHUNK_SIZE } from '../protocol/constants'
import {
    PortForwardSourceRequest, decodePortForwardData, decodePortForwardDestinationRequest,
    decodePortForwardDestinationResponse, encodePortForwardData,
    encodePortForwardDestinationRequest, encodePortForwardDestinationResponse,
} from '../protocol/messages'

type Send = (header: number, payload: Buffer) => void

interface SourceListener {
    config: ForwardedPortConfig
    server: Server
}

function describe (c: ForwardedPortConfig): string {
    return c.type === PortForwardType.Local
        ? `(local) ${c.host}:${c.port} -> (remote) localhost:${c.targetPort}`
        // ET lands reverse-tunnel traffic on the CLIENT's localhost:port; the
        // configured target address is ignored for TCP (PortForwardHandler::
        // createDestination connects to ::1 / 127.0.0.1), so say so.
        : `(remote) ${c.host}:${c.port} -> (local) localhost:${c.targetPort}`
}

function resolveAgentPath (): string|null {
    if (process.platform === 'win32') {
        return '\\\\.\\pipe\\openssh-ssh-agent'
    }
    return process.env.SSH_AUTH_SOCK ?? null
}

export class ETPortForwardHandler {
    /** Sockets we accepted locally, awaiting a socketId from the peer. Keyed by our token. */
    private unassigned = new Map<number, { socket: Socket, config: ForwardedPortConfig }>()
    /** socketId -> local socket, for tunnels where WE are the source. */
    private sourceSockets = new Map<number, Socket>()
    /** Which declared forward each source socket belongs to (for live removal). */
    private sourceSocketConfigs = new Map<number, ForwardedPortConfig>()
    /** socketId -> local socket, for tunnels where WE are the destination. */
    private destinationSockets = new Map<number, Socket>()
    private listeners: SourceListener[] = []
    /**
     * Destinations we declared in INITIAL_PAYLOAD. Inbound
     * PORT_FORWARD_DESTINATION_REQUEST packets make us open sockets, so every one
     * of them is checked against this list: a malicious etserver must not be able
     * to pivot us at arbitrary local or intranet ports (see §17.4).
     */
    private declaredReverseDestinations: { name?: string, port?: number }[] = []
    private nextToken = 1
    private nextSocketId = 1

    constructor (
        private logger: Logger,
        private send: Send,
        private emitServiceMessage: (msg: string) => void,
    ) {}

    // ---- setup ------------------------------------------------------------

    async startLocalForwards (configs: ForwardedPortConfig[]): Promise<void> {
        for (const config of configs.filter(x => x.type === PortForwardType.Local)) {
            try {
                await this.addLocalForward(config)
            } catch (err) {
                this.emitServiceMessage(`Failed to forward ${describe(config)}: ${err}`)
            }
        }
    }

    addLocalForward (config: ForwardedPortConfig): Promise<void> {
        return new Promise((resolve, reject) => {
            const server = createServer(socket => this.onLocalConnection(config, socket))
            server.once('error', reject)
            server.listen(config.port, config.host, () => {
                server.removeListener('error', reject)
                this.listeners.push({ config, server })
                this.emitServiceMessage(`Forwarding ${describe(config)}`)
                resolve()
            })
        })
    }

    removeForward (config: ForwardedPortConfig): void {
        const index = this.listeners.findIndex(x => x.config === config)
        if (index >= 0) {
            this.listeners[index].server.close()
            this.listeners.splice(index, 1)
            // The forward no longer exists from the user's point of view, so its
            // live tunnelled connections go too.
            for (const [socketId, owned] of this.sourceSocketConfigs) {
                if (owned === config) {
                    this.sourceSockets.get(socketId)?.destroy()
                }
            }
            for (const [token, entry] of this.unassigned) {
                if (entry.config === config) {
                    entry.socket.destroy()
                    this.unassigned.delete(token)
                }
            }
            this.emitServiceMessage(`Stopped forwarding ${describe(config)}`)
        }
    }

    /**
     * Reverse tunnels are declared in InitialPayload, not created at runtime.
     * Agent forwarding is just a reverse tunnel with an environment variable and NO source.
     */
    buildReverseTunnelRequests (options: ETProfileOptions): PortForwardSourceRequest[] {
        const requests: PortForwardSourceRequest[] = []
        this.declaredReverseDestinations = []

        for (const config of options.forwardedPorts.filter(x => x.type === PortForwardType.Remote)) {
            requests.push({
                source: { name: config.host, port: config.port },
                destination: { name: config.targetAddress, port: config.targetPort },
            })
            this.declaredReverseDestinations.push({ port: config.targetPort })
        }

        if (options.forwardAgent) {
            const authSock = resolveAgentPath()
            if (!authSock) {
                this.emitServiceMessage(
                    'Agent forwarding is enabled but no SSH agent was found; skipping it',
                )
            } else {
                // NOTE: source MUST be omitted here - etserver rejects a request that has
                // both a source and an environment variable.
                requests.push({
                    destination: { name: authSock },
                    environmentVariable: 'SSH_AUTH_SOCK',
                })
                this.declaredReverseDestinations.push({ name: authSock })
            }
        }

        return requests
    }

    // ---- we are the SOURCE ------------------------------------------------

    private onLocalConnection (config: ForwardedPortConfig, socket: Socket): void {
        const token = this.nextToken++
        // Pause until the peer assigns a socketId, otherwise early bytes are lost.
        socket.pause()
        this.unassigned.set(token, { socket, config })

        socket.once('error', () => {
            this.unassigned.delete(token)
            socket.destroy()
        })

        this.send(ETPacketType.PORT_FORWARD_DESTINATION_REQUEST, encodePortForwardDestinationRequest({
            // ET ignores the destination name for TCP and connects to the remote
            // localhost, but we send it anyway for forward compatibility.
            destination: { name: config.targetAddress, port: config.targetPort },
            fd: token,
        }))
    }

    private onDestinationResponse (payload: Buffer): void {
        const response = decodePortForwardDestinationResponse(payload)
        const token = response.clientFd ?? -1
        const entry = this.unassigned.get(token)
        this.unassigned.delete(token)

        if (!entry) {
            this.logger.warn(`Destination response for an unknown token ${token}`)
            return
        }
        if (response.hasError) {
            this.emitServiceMessage(`Remote refused a forwarded connection: ${response.error}`)
            entry.socket.destroy()
            return
        }
        if (response.socketId === undefined) {
            // Without a socketId we could not route PF_DATA for this socket;
            // writing one with the field omitted would corrupt the peer's maps.
            this.logger.warn('Destination response without a socket id; dropping the connection')
            entry.socket.destroy()
            return
        }

        const socketId = response.socketId
        const previous = this.sourceSockets.get(socketId)
        if (previous) {
            // socketIds are allocated by the peer (upstream uses rand()), so treat
            // a collision as "the peer considers the old one dead".
            previous.destroy()
        }
        this.sourceSockets.set(socketId, entry.socket)
        this.sourceSocketConfigs.set(socketId, entry.config)
        this.pipeSocket(entry.socket, socketId, true)
        entry.socket.resume()
    }

    // ---- we are the DESTINATION -------------------------------------------

    private onDestinationRequest (payload: Buffer): void {
        const request = decodePortForwardDestinationRequest(payload)
        const target = request.destination

        if (!this.isDeclaredReverseDestination(target)) {
            this.logger.warn('Rejected a port-forward destination that was not declared for this session')
            this.sendDestinationError(request.fd, new Error('Destination was not declared for this session'))
            return
        }

        if (target.port) {
            // Upstream always connects TCP destinations to the connecting side's
            // own localhost (::1, then 127.0.0.1) and ignores destination.name -
            // including on the client side for reverse tunnels. Mirror that.
            this.connectLocalhost(target.port, socket => this.onDestinationConnected(socket, request.fd), err =>
                this.sendDestinationError(request.fd, err))
        } else if (target.name) {
            // Unix socket path, or a Windows named pipe for agent forwarding.
            const socket = new Socket()
            socket.setNoDelay(true)
            socket.once('error', err => {
                socket.destroy()
                this.sendDestinationError(request.fd, err)
            })
            socket.connect(target.name, () => this.onDestinationConnected(socket, request.fd))
        } else {
            this.sendDestinationError(request.fd, new Error('Malformed port forward destination'))
        }
    }

    private onDestinationConnected (socket: Socket, fd: number): void {
        const socketId = this.nextSocketId++
        this.destinationSockets.set(socketId, socket)
        this.send(
            ETPacketType.PORT_FORWARD_DESTINATION_RESPONSE,
            encodePortForwardDestinationResponse({ clientFd: fd, socketId, hasError: false }),
        )
        this.pipeSocket(socket, socketId, false)
    }

    private sendDestinationError (fd: number, err: Error): void {
        this.send(
            ETPacketType.PORT_FORWARD_DESTINATION_RESPONSE,
            encodePortForwardDestinationResponse({ clientFd: fd, hasError: true, error: err.message }),
        )
    }

    /** ::1 first, then 127.0.0.1 - a fresh socket per attempt, matching upstream. */
    private connectLocalhost (port: number, onConnect: (socket: Socket) => void, onError: (err: Error) => void): void {
        const hosts = ['::1', '127.0.0.1']
        const attempt = (index: number): void => {
            const socket = new Socket()
            socket.setNoDelay(true)
            const fail = (err: Error) => {
                socket.destroy()
                if (index + 1 < hosts.length) {
                    attempt(index + 1)
                } else {
                    onError(err)
                }
            }
            socket.once('error', fail)
            socket.connect(port, hosts[index], () => {
                socket.removeListener('error', fail)
                onConnect(socket)
            })
        }
        attempt(0)
    }

    /**
     * A destination request is only honoured when it names something we declared
     * in INITIAL_PAYLOAD. TCP destinations are matched on port (the name is not
     * authoritative for TCP, and older servers may not echo it); Unix-socket
     * destinations such as agent forwarding are matched on the path.
     */
    private isDeclaredReverseDestination (target: { name?: string, port?: number }): boolean {
        return this.declaredReverseDestinations.some(d =>
            d.port !== undefined
                ? d.port === target.port
                : d.name !== undefined && d.name === target.name,
        )
    }

    // ---- shared plumbing --------------------------------------------------

    private pipeSocket (socket: Socket, socketId: number, sourceToDestination: boolean): void {
        socket.on('data', (data: Buffer) => {
            for (let o = 0; o < data.length; o += PORT_FORWARD_CHUNK_SIZE) {
                this.send(ETPacketType.PORT_FORWARD_DATA, encodePortForwardData({
                    sourceToDestination,
                    socketId,
                    buffer: data.subarray(o, o + PORT_FORWARD_CHUNK_SIZE),
                }))
            }
        })
        socket.on('end', () => {
            this.send(ETPacketType.PORT_FORWARD_DATA, encodePortForwardData({
                sourceToDestination, socketId, closed: true,
            }))
        })
        socket.on('error', err => {
            this.send(ETPacketType.PORT_FORWARD_DATA, encodePortForwardData({
                sourceToDestination, socketId, error: err.message,
            }))
            this.forget(socketId)
        })
        socket.on('close', () => this.forget(socketId))
    }

    private forget (socketId: number): void {
        this.sourceSockets.delete(socketId)
        this.sourceSocketConfigs.delete(socketId)
        this.destinationSockets.delete(socketId)
    }

    handlePacket (header: number, payload: Buffer): void {
        if (header === ETPacketType.PORT_FORWARD_DESTINATION_REQUEST) {
            this.onDestinationRequest(payload)
            return
        }
        if (header === ETPacketType.PORT_FORWARD_DESTINATION_RESPONSE) {
            this.onDestinationResponse(payload)
            return
        }
        // PORT_FORWARD_DATA
        const data = decodePortForwardData(payload)
        // sourceToDestination=true means "for whoever is the destination", i.e. us when
        // the peer is the source. The two maps keep the roles apart.
        const map = data.sourceToDestination ? this.destinationSockets : this.sourceSockets
        const socket = map.get(data.socketId)
        if (!socket) {
            this.logger.debug(`Data for a closed forwarded socket ${data.socketId}`)
            return
        }
        if (data.closed !== undefined || data.error !== undefined) {
            socket.destroy()
            this.forget(data.socketId)
            return
        }
        if (data.buffer?.length) {
            socket.write(data.buffer)
        }
    }

    dispose (): void {
        for (const l of this.listeners) {
            l.server.close()
        }
        this.listeners = []
        for (const e of [...this.unassigned.values()]) {
            e.socket.destroy()
        }
        for (const s of [...this.sourceSockets.values(), ...this.destinationSockets.values()]) {
            s.destroy()
        }
        this.unassigned.clear()
        this.sourceSockets.clear()
        this.sourceSocketConfigs.clear()
        this.destinationSockets.clear()
    }
}
