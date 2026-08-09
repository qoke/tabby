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
        : `(remote) ${c.host}:${c.port} -> (local) ${c.targetAddress}:${c.targetPort}`
}

function resolveAgentPath (): string|null {
    if (process.platform === 'win32') {
        return '\\\\.\\pipe\\openssh-ssh-agent'
    }
    return process.env.SSH_AUTH_SOCK ?? null
}

export class ETPortForwardHandler {
    /** Sockets we accepted locally, awaiting a socketId from the peer. Keyed by our token. */
    private unassigned = new Map<number, Socket>()
    /** socketId -> local socket, for tunnels where WE are the source. */
    private sourceSockets = new Map<number, Socket>()
    /** socketId -> local socket, for tunnels where WE are the destination. */
    private destinationSockets = new Map<number, Socket>()
    private listeners: SourceListener[] = []
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
            this.emitServiceMessage(`Stopped forwarding ${describe(config)}`)
        }
    }

    /**
     * Reverse tunnels are declared in InitialPayload, not created at runtime.
     * Agent forwarding is just a reverse tunnel with an environment variable and NO source.
     */
    buildReverseTunnelRequests (options: ETProfileOptions): PortForwardSourceRequest[] {
        const requests: PortForwardSourceRequest[] = []

        for (const config of options.forwardedPorts.filter(x => x.type === PortForwardType.Remote)) {
            requests.push({
                source: { name: config.host, port: config.port },
                destination: { name: config.targetAddress, port: config.targetPort },
            })
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
            }
        }

        return requests
    }

    // ---- we are the SOURCE ------------------------------------------------

    private onLocalConnection (config: ForwardedPortConfig, socket: Socket): void {
        const token = this.nextToken++
        // Pause until the peer assigns a socketId, otherwise early bytes are lost.
        socket.pause()
        this.unassigned.set(token, socket)

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
        const socket = this.unassigned.get(token)
        this.unassigned.delete(token)

        if (!socket) {
            this.logger.warn(`Destination response for an unknown token ${token}`)
            return
        }
        if (response.hasError) {
            this.emitServiceMessage(`Remote refused a forwarded connection: ${response.error}`)
            socket.destroy()
            return
        }

        const socketId = response.socketId!
        this.sourceSockets.set(socketId, socket)
        this.pipeSocket(socket, socketId, true)
        socket.resume()
    }

    // ---- we are the DESTINATION -------------------------------------------

    private onDestinationRequest (payload: Buffer): void {
        const request = decodePortForwardDestinationRequest(payload)
        const socketId = this.nextSocketId++
        const target = request.destination

        const socket = new Socket()
        const fail = (err: Error) => {
            socket.destroy()
            this.send(
                ETPacketType.PORT_FORWARD_DESTINATION_RESPONSE,
                encodePortForwardDestinationResponse({
                    clientFd: request.fd, hasError: true, error: err.message,
                }),
            )
        }
        socket.once('error', fail)

        const onConnect = () => {
            socket.removeListener('error', fail)
            this.destinationSockets.set(socketId, socket)
            this.send(
                ETPacketType.PORT_FORWARD_DESTINATION_RESPONSE,
                encodePortForwardDestinationResponse({
                    clientFd: request.fd, socketId, hasError: false,
                }),
            )
            this.pipeSocket(socket, socketId, false)
        }

        if (target.port) {
            socket.connect(target.port, target.name ?? '127.0.0.1', onConnect)
        } else if (target.name) {
            // Unix socket path, or a Windows named pipe for agent forwarding.
            socket.connect(target.name, onConnect)
        } else {
            fail(new Error('Malformed port forward destination'))
        }
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
            map.delete(data.socketId)
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
        for (const s of [...this.unassigned.values(), ...this.sourceSockets.values(), ...this.destinationSockets.values()]) {
            s.destroy()
        }
        this.unassigned.clear()
        this.sourceSockets.clear()
        this.destinationSockets.clear()
    }
}
