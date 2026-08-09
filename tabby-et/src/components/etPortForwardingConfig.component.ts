/* eslint-disable @typescript-eslint/explicit-module-boundary-types */
import { Component, Input, Output, EventEmitter } from '@angular/core'
import { ForwardedPortConfig, PortForwardType } from 'tabby-ssh'
import { parseTunnelSpec } from '../session/tunnelSpec'

/** @hidden */
@Component({
    selector: 'et-port-forwarding-config',
    templateUrl: './etPortForwardingConfig.component.pug',
})
export class ETPortForwardingConfigComponent {
    @Input() model: ForwardedPortConfig[]
    @Output() forwardAdded = new EventEmitter<ForwardedPortConfig>()
    @Output() forwardRemoved = new EventEmitter<ForwardedPortConfig>()
    newForward: ForwardedPortConfig
    spec = ''
    specError: string|null = null
    /** ET has no Dynamic forwarding. */
    PortForwardType = PortForwardType

    constructor () {
        this.reset()
    }

    reset (): void {
        this.newForward = {
            type: PortForwardType.Local,
            host: '127.0.0.1',
            port: 8000,
            targetAddress: 'localhost',
            targetPort: 80,
            description: '',
        }
    }

    addForward (): void {
        this.forwardAdded.emit(this.newForward)
        this.reset()
    }

    remove (fw: ForwardedPortConfig): void {
        this.forwardRemoved.emit(fw)
        this.newForward = fw
    }

    importSpec (): void {
        this.specError = null
        try {
            for (const fw of parseTunnelSpec(this.spec, this.newForward.type)) {
                this.forwardAdded.emit(fw)
            }
            this.spec = ''
        } catch (e) {
            this.specError = e.message
        }
    }
}
