import { Socket } from 'net'

/**
 * Pull-based reader over a net.Socket.
 *
 * One ByteReader belongs to exactly one socket. When the socket dies, every pending
 * and future read rejects - which is how the packet read loop learns to stop.
 */
export class ByteReader {
    private buffer: Buffer = Buffer.alloc(0)
    private waiter: { size: number, timer: any, resolve: (b: Buffer) => void, reject: (e: Error) => void }|null = null
    private failure: Error|null = null

    constructor (socket: Socket) {
        socket.on('data', data => this.onData(data))
        socket.on('error', err => this.fail(err))
        socket.on('close', () => this.fail(new Error('Connection closed')))
        socket.on('end', () => this.fail(new Error('Connection ended by remote')))
    }

    private onData (data: Buffer): void {
        this.buffer = this.buffer.length ? Buffer.concat([this.buffer, data]) : data
        this.pump()
    }

    private pump (): void {
        if (this.waiter && this.buffer.length >= this.waiter.size) {
            const { size, resolve } = this.waiter
            this.clearTimer()
            this.waiter = null
            const out = this.buffer.subarray(0, size)
            this.buffer = this.buffer.subarray(size)
            resolve(out)
        }
    }

    private fail (err: Error): void {
        if (this.failure) {
            return
        }
        this.failure = err
        const w = this.waiter
        this.waiter = null
        // The timer must die with the waiter, or it keeps the event loop (and a
        // doomed reject closure) alive for its full duration.
        if (w?.timer) {
            clearTimeout(w.timer)
        }
        w?.reject(err)
    }

    private clearTimer (): void {
        if (this.waiter?.timer) {
            clearTimeout(this.waiter.timer)
        }
    }

    /** Read exactly `size` bytes. Rejects on socket failure or timeout. */
    read (size: number, timeoutMs?: number): Promise<Buffer> {
        if (size === 0) {
            return Promise.resolve(Buffer.alloc(0))
        }
        if (this.buffer.length >= size) {
            const out = this.buffer.subarray(0, size)
            this.buffer = this.buffer.subarray(size)
            return Promise.resolve(out)
        }
        if (this.failure) {
            return Promise.reject(this.failure)
        }
        if (this.waiter) {
            return Promise.reject(new Error('ByteReader: concurrent reads are not supported'))
        }
        return new Promise<Buffer>((resolve, reject) => {
            this.waiter = {
                size,
                timer: null,
                resolve: b => {
                    this.clearTimer()
                    resolve(b)
                },
                reject: e => {
                    this.clearTimer()
                    reject(e)
                },
            }
            if (timeoutMs) {
                // A timed-out read does not consume the buffer: the bytes (if any
                // ever arrive) stay available to a later read, and the timeout
                // does not tear the socket down. Callers decide what a timeout
                // means for the connection.
                this.waiter.timer = setTimeout(() => {
                    const w = this.waiter
                    this.waiter = null
                    w?.reject(new Error(`Timed out waiting for ${size} bytes from the ET server`))
                }, timeoutMs)
            }
        })
    }

    dispose (): void {
        this.fail(new Error('Reader disposed'))
    }
}
