/* eslint-disable @typescript-eslint/no-unsafe-enum-comparison */
import { Socket } from 'net'
import { Observable, Subject } from 'rxjs'
import { Logger } from 'tabby-core'

import { BackedReader, ETPacket } from './backedReader'
import { BackedWriter } from './backedWriter'
import { ByteReader } from './byteReader'
import { ETCrypto } from './crypto'
import {
    CLIENT_SERVER_NONCE_MSB, ETConnectStatus, HANDSHAKE_TIMEOUT,
    MAX_HANDSHAKE_PROTO_LENGTH, MAX_PROTO_LENGTH, PROTOCOL_VERSION,
    RECONNECT_INTERVAL, SERVER_CLIENT_NONCE_MSB,
} from './constants'
import {
    decodeCatchupBuffer, decodeConnectResponse, decodeSequenceHeader,
    encodeCatchupBuffer, encodeConnectRequest, encodeSequenceHeader,
} from './messages'

export type ETConnectionState = 'connecting'|'connected'|'reconnecting'|'ended'

export interface ETConnectionOptions {
    host: string
    port: number
    id: string
    passkey: string
    /** 0 = retry forever, matching the reference client. */
    maxReconnectAttempts: number
}

export class ETClientConnection {
    get state$ (): Observable<ETConnectionState> { return this.stateSubject }
    get packet$ (): Observable<ETPacket> { return this.packetSubject }
    /** Emits once with a human-readable reason, then completes. */
    get ended$ (): Observable<string> { return this.endedSubject }

    state: ETConnectionState = 'connecting'

    private stateSubject = new Subject<ETConnectionState>()
    private packetSubject = new Subject<ETPacket>()
    private endedSubject = new Subject<string>()

    private socket: Socket|null = null
    private byteReader: ByteReader|null = null
    private reader: BackedReader
    private writer: BackedWriter
    private shuttingDown = false
    private reconnectAttempts = 0

    constructor (
        private options: ETConnectionOptions,
        private logger: Logger,
    ) {
        this.reader = new BackedReader(new ETCrypto(options.passkey, SERVER_CLIENT_NONCE_MSB))
        this.writer = new BackedWriter(new ETCrypto(options.passkey, CLIENT_SERVER_NONCE_MSB))
    }

    // ---- lifecycle --------------------------------------------------------

    /** Initial connection. Throws on a fatal handshake failure. */
    async connect (): Promise<void> {
        this.setState('connecting')
        const socket = await this.openSocket()
        const byteReader = new ByteReader(socket)
        const status = await this.sendConnectRequest(socket, byteReader)

        if (status !== ETConnectStatus.NEW_CLIENT && status !== ETConnectStatus.RETURNING_CLIENT) {
            socket.destroy()
            throw new Error(this.describeStatus(status))
        }

        this.attach(socket, byteReader)
        this.setState('connected')
        this.runReadLoop()
    }

    /** Send an encrypted packet. Buffers while disconnected. */
    writePacket (header: number, payload: Buffer): void {
        if (this.shuttingDown) {
            return
        }
        if (!this.writer.write(header, payload)) {
            this.logger.warn('ET write buffer is full; dropping a packet')
        }
    }

    /** Deliberately drop the TCP connection to exercise recovery. */
    forceReconnect (): void {
        this.logger.info('Forcing an ET reconnect')
        this.dropSocketAndReconnect()
    }

    shutdown (): void {
        this.shuttingDown = true
        this.socket?.destroy()
        this.socket = null
        this.byteReader?.dispose()
        this.setState('ended')
        this.endedSubject.complete()
        this.packetSubject.complete()
        this.stateSubject.complete()
    }

    // ---- internals --------------------------------------------------------

    private setState (state: ETConnectionState): void {
        if (this.state === state) {
            return
        }
        this.state = state
        this.stateSubject.next(state)
    }

    private attach (socket: Socket, byteReader: ByteReader): void {
        this.socket = socket
        this.byteReader = byteReader
        this.writer.attach(socket)
        this.reader.attach(byteReader)
        this.reconnectAttempts = 0
    }

    private openSocket (): Promise<Socket> {
        return new Promise((resolve, reject) => {
            const socket = new Socket()
            socket.setNoDelay(true)
            const onError = (err: Error) => {
                socket.destroy()
                reject(err)
            }
            socket.once('error', onError)
            socket.connect(this.options.port, this.options.host, () => {
                socket.removeListener('error', onError)
                resolve(socket)
            })
        })
    }

    /** Framing A: 8-byte little-endian length + protobuf. */
    private async writeProto (socket: Socket, message: Buffer): Promise<void> {
        const header = Buffer.allocUnsafe(8)
        header.writeBigInt64LE(BigInt(message.length), 0)
        socket.write(header)
        if (message.length) {
            socket.write(message)
        }
    }

    private async readProto (reader: ByteReader, maxLength = MAX_PROTO_LENGTH): Promise<Buffer> {
        const header = await reader.read(8, HANDSHAKE_TIMEOUT)
        const length = Number(header.readBigInt64LE(0))
        if (length < 0 || length > maxLength) {
            throw new Error(`Invalid ET handshake message length ${length}`)
        }
        // A zero-length frame is legal and means "default-constructed message".
        return length === 0 ? Buffer.alloc(0) : reader.read(length, HANDSHAKE_TIMEOUT)
    }

    private async sendConnectRequest (socket: Socket, reader: ByteReader): Promise<number> {
        await this.writeProto(socket, encodeConnectRequest(this.options.id, PROTOCOL_VERSION))
        const response = decodeConnectResponse(await this.readProto(reader, MAX_HANDSHAKE_PROTO_LENGTH))
        if (response.error) {
            this.logger.info(`etserver said: ${response.error}`)
        }
        return response.status ?? 0
    }

    private describeStatus (status: number): string {
        switch (status) {
            case ETConnectStatus.INVALID_KEY:
                return 'The ET server rejected our session key. The remote session has ended.'
            case ETConnectStatus.MISMATCHED_PROTOCOL:
                return `Protocol version mismatch: Tabby speaks ET protocol ${PROTOCOL_VERSION}. Upgrade or downgrade the remote etserver so both sides match.`
            default:
                return `The ET server refused the connection (status ${status})`
        }
    }

    private async runReadLoop (): Promise<void> {
        try {
            for (;;) {
                const packet = await this.reader.read()
                this.packetSubject.next(packet)
            }
        } catch (err) {
            if (this.shuttingDown) {
                return
            }
            this.logger.info(`ET read loop stopped: ${err}`)
            this.dropSocketAndReconnect()
        }
    }

    private dropSocketAndReconnect (): void {
        if (this.shuttingDown || this.state === 'reconnecting') {
            return
        }
        this.socket?.destroy()
        this.socket = null
        this.byteReader?.dispose()
        this.byteReader = null
        this.writer.detach()
        this.setState('reconnecting')
        void this.reconnectLoop()
    }

    private async reconnectLoop (): Promise<void> {
        for (;;) {
            if (this.shuttingDown) {
                return
            }
            await new Promise(resolve => setTimeout(resolve, RECONNECT_INTERVAL))
            // eslint-disable-next-line @typescript-eslint/no-unnecessary-condition
            if (this.shuttingDown) {
                return
            }
            this.reconnectAttempts++
            try {
                const socket = await this.openSocket()
                const byteReader = new ByteReader(socket)
                const status = await this.sendConnectRequest(socket, byteReader)

                if (status === ETConnectStatus.INVALID_KEY) {
                    // The only way the client learns the remote shell has exited.
                    socket.destroy()
                    this.end('Session terminated by the server')
                    return
                }
                if (status !== ETConnectStatus.RETURNING_CLIENT) {
                    this.logger.warn(`Unexpected reconnect status ${status}; retrying`)
                    socket.destroy()
                    continue
                }

                await this.recover(socket, byteReader)
                this.attach(socket, byteReader)
                this.setState('connected')
                this.runReadLoop()
                return
            } catch (err) {
                this.logger.debug(`ET reconnect attempt ${this.reconnectAttempts} failed: ${err}`)
                if (this.options.maxReconnectAttempts > 0 && this.reconnectAttempts >= this.options.maxReconnectAttempts) {
                    this.end(`Could not resume the session after ${this.reconnectAttempts} attempts`)
                    return
                }
            }
        }
    }

    /**
     * Symmetric catch-up exchange. Order matters and must be:
     *   write SequenceHeader -> read SequenceHeader -> write CatchupBuffer -> read CatchupBuffer
     * Both peers write before reading, so this cannot deadlock.
     */
    private async recover (socket: Socket, byteReader: ByteReader): Promise<void> {
        await this.writeProto(socket, encodeSequenceHeader(this.reader.sequenceNumber))

        const remoteSequence = decodeSequenceHeader(
            await this.readProto(byteReader, MAX_HANDSHAKE_PROTO_LENGTH),
        )

        const toSend = this.writer.recover(remoteSequence)
        await this.writeProto(socket, encodeCatchupBuffer(toSend))

        const recovered = decodeCatchupBuffer(await this.readProto(byteReader))

        this.reader.revive(byteReader, recovered)
        this.writer.attach(socket)

        this.logger.info(`ET session resumed: replayed ${toSend.length} out, ${recovered.length} in`)
    }

    private end (reason: string): void {
        this.shuttingDown = true
        this.setState('ended')
        this.endedSubject.next(reason)
        this.endedSubject.complete()
    }
}
