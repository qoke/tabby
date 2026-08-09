/* eslint-disable @typescript-eslint/explicit-module-boundary-types */
import { Component, Input } from '@angular/core'
import { ForwardedPortConfig, PortForwardType } from 'tabby-ssh'
import { ETSession } from '../session/etSession'

/** @hidden */
@Component({
    templateUrl: './etPortForwardingModal.component.pug',
})
export class ETPortForwardingModalComponent {
    @Input() session: ETSession

    get model (): ForwardedPortConfig[] {
        return this.session.profile.options.forwardedPorts
    }

    async onForwardAdded (fw: ForwardedPortConfig): Promise<void> {
        if (fw.type === PortForwardType.Remote) {
            // Reverse tunnels are fixed at INITIAL_PAYLOAD time by the protocol.
            this.session.emitServiceMessage(
                'Remote forwards can only be added before connecting. Add it to the profile and reconnect.',
            )
            return
        }
        try {
            await this.session.forwards.addLocalForward(fw)
            this.model.push(fw)
        } catch (e) {
            this.session.emitServiceMessage(`Could not forward port: ${e.message}`)
        }
    }

    onForwardRemoved (fw: ForwardedPortConfig): void {
        this.session.forwards.removeForward(fw)
        const index = this.model.indexOf(fw)
        if (index >= 0) {
            this.model.splice(index, 1)
        }
    }
}
