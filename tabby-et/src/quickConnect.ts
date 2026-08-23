import { DEFAULT_ET_PORT } from './protocol/constants'

export interface ETQuickConnectTarget {
    host: string
    user?: string
    port: number
}

/**
 * Pure parser behind ETProfilesService.quickConnect.
 *
 * Accepts "user@host", "user@host:2022", "user@[::1]:2022", and tolerates a
 * leading "et " or "et://" so users can paste a command line. Never throws and
 * never produces a non-finite port: unparsable port parts fall back to the
 * default, and unbalanced brackets degrade to a best-effort host.
 */
export function parseQuickConnectQuery (query: string): ETQuickConnectTarget {
    let raw = query.trim().replace(/^et:\/\//, '').replace(/^et\s+/, '')
    let user: string|undefined = undefined
    let port = DEFAULT_ET_PORT

    if (raw.includes('@')) {
        const parts = raw.split(/@/g)
        raw = parts[parts.length - 1]
        user = parts.slice(0, parts.length - 1).join('@')
    }
    if (raw.includes('[')) {
        // Bracketed IPv6, optionally ":port" after the ']'. The ']' may be
        // missing in malformed input - treat the whole remainder as the host
        // instead of crashing on undefined.
        const after = raw.split(']')
        const portPart = after.length > 1 ? after[1].substring(1) : ''
        const parsed = parseInt(portPart, 10)
        if (!isNaN(parsed)) {
            port = parsed
        }
        raw = after[0].substring(1)
    } else if ((raw.match(/:/g) ?? []).length === 1) {
        // Exactly one ':' means host:port. Bare IPv6 hosts (::1) contain
        // multiple colons and never carry a port in this shorthand.
        const parsed = parseInt(raw.split(':')[1], 10)
        if (!isNaN(parsed)) {
            port = parsed
        }
        raw = raw.split(':')[0]
    }

    return { host: raw, user, port }
}
