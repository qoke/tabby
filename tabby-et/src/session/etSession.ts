import { Injector } from '@angular/core'
import colors from 'ansi-colors'
import { Socket } from 'net'
import { Observable, ReplaySubject, Subject } from 'rxjs'
import stripAnsi from 'strip-ansi'
import { LogService } from 'tabby-core'
import { BaseSession, InputProcessor, UTF8SplitterMiddleware } from 'tabby-terminal'
import { KeyboardInteractivePrompt, SSHSession } from 'tabby-ssh'

import { ETProfile } from '../api/interfaces'
import { ETClientConnection, ETConnectionState } from '../protocol/connection'
import {
    ETPacketType, INITIAL_RESPONSE_TIMEOUT, PING_TIMEOUT, TERMINAL_CHUNK_SIZE,
} from '../protocol/constants'
import {
    decodeInitialResponse, decodeTerminalBuffer, encodeInitialPayload,
    encodeTerminalBuffer, encodeTerminalInfo,
} from '../protocol/messages'
import { ETBootstrap } from './bootstrap'
import { ETPortForwardHandler } from './portForwarding'

export class ETSession extends BaseSession {
    get serviceMessage$ (): Observable<string> { return this.serviceMessage }
    get keyboardInteractivePrompt$ (): Observable<KeyboardInteractivePrompt> { return this.kiPrompt }
    get connectionState$ (): Observable<ETConnectionState> { return this.connectionStateSubject }
    /** The bootstrap SSH session, needed by the keyboard-interactive panel. */
    get bootstrapSession$ (): Observable<SSHSession> { return this.bootstrapSession }

    connectionState: ETConnectionState = 'connecting'
    forwards: ETPortForwardHandler

    private serviceMessage = new Subject<string>()
    private kiPrompt = new Subject<KeyboardInteractivePrompt>()
    private connectionStateSubject = new Subject<ETConnectionState>()
    private bootstrapSession = new ReplaySubject<SSHSession>(1)

    private connection: ETClientConnection|null = null
    private bootstrap: ETBootstrap|null = null
    private keepaliveTimer: any = null
    private awaitingKeepalive = false
    private lastSize = { columns: 0, rows: 0 }
    private initialResponse: { resolve: () => void, reject: (e: Error) => void }|null = null

    constructor (
        private injector: Injector,
        public profile: ETProfile,
    ) {
        super(injector.get(LogService).create(`et-${profile.options.host}`))
        this.setLoginScriptsOptions(profile.options)
        this.middleware.push(new UTF8SplitterMiddleware())
        this.middleware.push(new InputProcessor(profile.options.input))

        this.forwards = new ETPortForwardHandler(
            this.logger,
            (header, payload) => this.connection?.writePacket(header, payload),
            msg => this.emitServiceMessage(msg),
        )
    }

    // ---- lifecycle --------------------------------------------------------

    async start (): Promise<void> {
        const o = this.profile.options
        const port = o.port

        // 1. Fail fast if etserver is unreachable, exactly as `et` does.
        await this.ping(o.host, port)

        // 2. Bootstrap over SSH.
        this.emitServiceMessage(colors.bgBlue.black(' SSH ') + ' Starting the remote session')
        this.bootstrap = new ETBootstrap(this.injector, this.profile)
        this.bootstrap.sshSessionCreated$.subscribe(s => {
            this.bootstrapSession.next(s)
            s.serviceMessage$.subscribe(m => this.emitServiceMessage(m))
            s.keyboardInteractivePrompt$.subscribe(p => this.kiPrompt.next(p))
        })

        let credentials = await this.bootstrap.run()

        // 2b. ET-native jump host: bootstrap the jump host with the same credentials.
        if (o.jumpHost) {
            this.emitServiceMessage(colors.bgBlue.black(' JUMP ') + ` Preparing ${o.jumpHost}`)
            credentials = await this.bootstrap.run({
                credentials,
                jumpTo: { host: o.host, port },
            })
        }

        // 3. Connect and authenticate.
        const target = o.jumpHost
            ? { host: o.jumpHost, port: o.jumpPort }
            : { host: o.host, port }

        this.connection = new ETClientConnection({
            host: target.host,
            port: target.port,
            id: credentials.id,
            passkey: credentials.passkey,
            maxReconnectAttempts: o.maxReconnectAttempts,
        }, this.logger)

        this.connection.packet$.subscribe(p => this.handlePacket(p.header, p.payload))
        this.connection.state$.subscribe(s => this.onConnectionState(s))
        this.connection.ended$.subscribe(reason => {
            this.emitServiceMessage(colors.bgRed.black(' X ') + ` ${reason}`)
            this.destroy()
        })

        await this.connection.connect()

        // 4. INITIAL_PAYLOAD / INITIAL_RESPONSE.
        await this.sendInitialPayload()

        // 5. Local listeners for forward tunnels.
        await this.forwards.startLocalForwards(o.forwardedPorts)

        // 6. Go live.
        this.open = true
        this.connectionState = 'connected'
        this.sendTerminalInfo()
        this.startKeepalive()
        this.loginScriptProcessor?.executeUnconditionalScripts()
    }

    private ping (host: string, port: number): Promise<void> {
        return new Promise((resolve, reject) => {
            const socket = new Socket()
            const fail = () => {
                socket.destroy()
                reject(new Error(
                    `Could not reach the ET server at ${host}:${port}. `
                    + 'Check that etserver is running and the port is open.',
                ))
            }
            socket.setTimeout(PING_TIMEOUT)
            socket.once('timeout', fail)
            socket.once('error', fail)
            socket.connect(port, host, () => {
                socket.destroy()
                resolve()
            })
        })
    }

    private sendInitialPayload (): Promise<void> {
        const o = this.profile.options
        const payload = encodeInitialPayload({
            jumphost: !!o.jumpHost,
            reverseTunnels: this.forwards.buildReverseTunnelRequests(o),
            environmentVariables: o.environmentVariables,
        })

        const promise = new Promise<void>((resolve, reject) => {
            this.initialResponse = { resolve, reject }
            setTimeout(
                () => this.initialResponse?.reject(new Error('The ET server did not acknowledge the session')),
                INITIAL_RESPONSE_TIMEOUT,
            )
        })
        this.connection!.writePacket(ETPacketType.INITIAL_PAYLOAD, payload)
        return promise
    }

    // ---- packet routing ---------------------------------------------------

    private handlePacket (header: number, payload: Buffer): void {
        switch (header) {
            case ETPacketType.TERMINAL_BUFFER:
                this.emitOutput(decodeTerminalBuffer(payload))
                this.resetKeepalive()
                break

            case ETPacketType.KEEP_ALIVE:
                this.awaitingKeepalive = false
                break

            case ETPacketType.INITIAL_RESPONSE: {
                const response = decodeInitialResponse(payload)
                const pending = this.initialResponse
                this.initialResponse = null
                if (response.hasError) {
                    pending?.reject(new Error(`The ET server refused the session: ${response.error}`))
                } else {
                    pending?.resolve()
                }
                break
            }

            case ETPacketType.PORT_FORWARD_DESTINATION_REQUEST:
            case ETPacketType.PORT_FORWARD_DESTINATION_RESPONSE:
            case ETPacketType.PORT_FORWARD_DATA:
                this.forwards.handlePacket(header, payload)
                this.resetKeepalive()
                break

            default:
                // Do NOT throw: an unknown header must not kill the session.
                this.logger.warn(`Ignoring unknown ET packet type ${header}`)
        }
    }

    private onConnectionState (state: ETConnectionState): void {
        this.connectionState = state
        this.connectionStateSubject.next(state)
        if (state === 'reconnecting') {
            this.emitServiceMessage(
                colors.bgYellow.black(' ~ ') + ' Connection lost, attempting to resume the session...',
            )
        }
        if (state === 'connected' && this.open) {
            this.emitServiceMessage(colors.bgGreen.black(' OK ') + ' Session resumed')
            // The remote PTY size may have been changed by another client.
            this.sendTerminalInfo(true)
        }
    }

    // ---- BaseSession contract ---------------------------------------------

    write (data: Buffer): void {
        if (!this.connection) {
            return
        }
        // Chunk to match ET's own 16 KiB reads.
        for (let offset = 0; offset < data.length; offset += TERMINAL_CHUNK_SIZE) {
            const chunk = data.subarray(offset, offset + TERMINAL_CHUNK_SIZE)
            this.connection.writePacket(ETPacketType.TERMINAL_BUFFER, encodeTerminalBuffer(chunk))
        }
        this.resetKeepalive()
    }

    resize (columns: number, rows: number): void {
        if (!columns || !rows) {
            return
        }
        this.lastSize = { columns, rows }
        this.sendTerminalInfo()
    }

    private sendTerminalInfo (force = false): void {
        if (!this.connection || !this.lastSize.columns) {
            return
        }
        if (!force && !this.open) {
            return
        }
        this.connection.writePacket(ETPacketType.TERMINAL_INFO, encodeTerminalInfo({
            row: this.lastSize.rows,
            column: this.lastSize.columns,
            width: 0,
            height: 0,
        }))
    }

    kill (_signal?: string): void {
        // ET has no "kill the remote shell" packet. Closing the socket only detaches;
        // the remote session survives until its shell exits. See ETERNAL_TERMINAL.md D9.
        this.connection?.shutdown()
    }

    async destroy (): Promise<void> {
        this.stopKeepalive()
        this.forwards.dispose()
        this.connection?.shutdown()
        this.connection = null
        this.serviceMessage.complete()
        this.kiPrompt.complete()
        this.connectionStateSubject.complete()
        await super.destroy()
    }

    async gracefullyKillProcess (): Promise<void> {
        this.kill()
    }

    supportsWorkingDirectory (): boolean {
        return !!this.reportedCWD
    }

    async getWorkingDirectory (): Promise<string|null> {
        return this.reportedCWD ?? null
    }

    async getChildProcesses (): Promise<any[]> {
        return []
    }

    emitServiceMessage (msg: string): void {
        this.serviceMessage.next(msg)
        this.logger.info(stripAnsi(msg))
    }

    // ---- keepalive --------------------------------------------------------

    private startKeepalive (): void {
        const interval = Math.min(Math.max(this.profile.options.keepaliveInterval, 1), 5) * 1000
        this.stopKeepalive()
        this.keepaliveTimer = setInterval(() => {
            if (!this.connection || this.connection.state !== 'connected') {
                this.awaitingKeepalive = false
                return
            }
            if (this.awaitingKeepalive) {
                this.logger.info('Missed a keepalive; forcing a reconnect')
                this.awaitingKeepalive = false
                this.connection.forceReconnect()
                return
            }
            this.connection.writePacket(ETPacketType.KEEP_ALIVE, Buffer.alloc(0))
            this.awaitingKeepalive = true
        }, interval)
    }

    private resetKeepalive (): void {
        this.awaitingKeepalive = false
    }

    private stopKeepalive (): void {
        if (this.keepaliveTimer) {
            clearInterval(this.keepaliveTimer)
            this.keepaliveTimer = null
        }
    }

    /** Exposed for the "Force reconnect" hotkey and for tests. */
    forceReconnect (): void {
        this.connection?.forceReconnect()
    }
}
