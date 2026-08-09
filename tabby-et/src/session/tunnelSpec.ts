import { ForwardedPortConfig, PortForwardType } from 'tabby-ssh'

function parseRange (s: string): number[] {
    if (!s.includes('-')) {
        return [parseInt(s)]
    }
    const [from, to] = s.split('-').map(x => parseInt(x))
    if (isNaN(from) || isNaN(to) || to < from) {
        throw new Error(`Invalid port range "${s}"`)
    }
    const out: number[] = []
    for (let p = from; p <= to; p++) {
        out.push(p)
    }
    return out
}

function make (
    type: PortForwardType, host: string, port: number,
    targetAddress: string, targetPort: number, description: string,
): ForwardedPortConfig {
    if (isNaN(port) || isNaN(targetPort)) {
        throw new Error(`Invalid port in "${description}"`)
    }
    return { type, host, port, targetAddress, targetPort, description }
}

/**
 * Parse an ET tunnel spec into individual forwards.
 * Supported: "8080:80", "8080-8089:9080-9089", "bind:8080:host:80".
 * Ranges must be equal length on both sides. Unix-socket specs are rejected here
 * because the UI models TCP forwards only.
 */
export function parseTunnelSpec (spec: string, type: PortForwardType): ForwardedPortConfig[] {
    const out: ForwardedPortConfig[] = []
    for (const element of spec.split(',').map(x => x.trim()).filter(x => x)) {
        const parts = element.split(':')

        if (parts.length === 4) {
            out.push(make(type, parts[0], parseInt(parts[1]), parts[2], parseInt(parts[3]), element))
            continue
        }
        if (parts.length !== 2) {
            throw new Error(`Cannot parse tunnel "${element}". Use PORT:PORT or BIND:PORT:HOST:PORT.`)
        }

        const [left, right] = parts
        if (left.includes('-') || right.includes('-')) {
            const l = parseRange(left)
            const r = parseRange(right)
            if (l.length !== r.length) {
                throw new Error(`Port ranges in "${element}" have different lengths`)
            }
            for (let i = 0; i < l.length; i++) {
                out.push(make(type, '127.0.0.1', l[i], 'localhost', r[i], element))
            }
            continue
        }
        out.push(make(type, '127.0.0.1', parseInt(left), 'localhost', parseInt(right), element))
    }
    return out
}
