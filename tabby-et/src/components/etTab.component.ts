import { marker as _ } from '@biesbjerg/ngx-translate-extract-marker'
import colors from 'ansi-colors'
import { Component, HostListener, Injector } from '@angular/core'
import { NgbModal } from '@ng-bootstrap/ng-bootstrap'
import { Platform } from 'tabby-core'
import { BaseTerminalTabComponent, ConnectableTerminalTabComponent } from 'tabby-terminal'
import { KeyboardInteractivePrompt, SSHProfile } from 'tabby-ssh'

import { ETProfile } from '../api/interfaces'
import { ETSession } from '../session/etSession'
import { ETConnectionState } from '../protocol/connection'
import { ETPortForwardingModalComponent } from './etPortForwardingModal.component'

/** @hidden */
@Component({
    selector: 'et-tab',
    template: `${BaseTerminalTabComponent.template} ${require('./etTab.component.pug')}`,
    styles: [...BaseTerminalTabComponent.styles, require('./etTab.component.scss')],
    animations: BaseTerminalTabComponent.animations,
})
export class ETTabComponent extends ConnectableTerminalTabComponent<ETProfile> {
    Platform = Platform
    session: ETSession|null = null
    connectionState: ETConnectionState = 'connecting'
    activeKIPrompt: KeyboardInteractivePrompt|null = null
    /** The synthesised SSH profile, needed by the keyboard-interactive panel. */
    bootstrapProfile: SSHProfile|null = null

    constructor (
        injector: Injector,
        private ngbModal: NgbModal,
    ) {
        super(injector)
        this.enableToolbar = true
        this.sessionChanged$.subscribe(() => {
            this.activeKIPrompt = null
        })
    }

    ngOnInit (): void {
        this.subscribeUntilDestroyed(this.hotkeys.hotkey$, hotkey => {
            if (!this.hasFocus) {
                return
            }
            switch (hotkey) {
                case 'restart-et-session':
                    this.reconnect()
                    break
                case 'et-force-reconnect':
                    this.session?.forceReconnect()
                    break
            }
        })
        super.ngOnInit()
    }

    async initializeSession (): Promise<void> {
        await super.initializeSession()

        const session = new ETSession(this.injector, this.profile)
        this.setSession(session)

        this.attachSessionHandler(session.serviceMessage$, msg => {
            const formatted = msg.replace(/\n/g, '\r\n      ')
            this.write(`\r${colors.black.bgWhite(' ET ')} ${formatted}\r\n`)
        })
        this.attachSessionHandler(session.connectionState$, state => {
            this.connectionState = state
        })
        this.attachSessionHandler(session.keyboardInteractivePrompt$, prompt => {
            this.activeKIPrompt = prompt
            setTimeout(() => this.frontend?.scrollToBottom())
        })
        // The KI panel needs the SSH profile the bootstrap actually used, so it can
        // look up and offer to save the password.
        this.attachSessionHandler(session.bootstrapSession$, s => {
            this.bootstrapProfile = s.profile
        })

        this.startSpinner(this.translate.instant(_('Connecting')))
        try {
            await session.start()
            this.session?.resize(this.size.columns, this.size.rows)
        } catch (e) {
            this.write(colors.black.bgRed(' X ') + ' ' + colors.red(e.message) + '\r\n')
            // A session that failed mid-start() never set open=true, so the tab's
            // close path would skip BaseSession.destroy() and leak its timers and
            // local port listeners. Tear it down here instead.
            await session.destroy().catch(() => { /* already down */ })
        } finally {
            this.stopSpinner()
        }
    }

    protected onSessionDestroyed (): void {
        if (this.frontend) {
            this.write('\r\n' + colors.black.bgWhite(' ET ') + ` ${this.profile.options.host}: session closed\r\n`)
            super.onSessionDestroyed()
        }
    }

    showPortForwarding (): void {
        if (!this.session) {
            return
        }
        const modal = this.ngbModal.open(ETPortForwardingModalComponent)
            .componentInstance as ETPortForwardingModalComponent
        modal.session = this.session
    }

    async canClose (): Promise<boolean> {
        if (!this.session?.open) {
            return true
        }
        if (!(this.profile.options.warnOnClose ?? this.config.store.et.warnOnClose)) {
            return true
        }
        return (await this.platform.showMessageBox({
            type: 'warning',
            message: this.translate.instant(
                _('Detach from {host}? The remote session will keep running.'),
                this.profile.options,
            ),
            buttons: [this.translate.instant(_('Detach')), this.translate.instant(_('Do not close'))],
            defaultId: 0,
            cancelId: 1,
        })).response === 0
    }

    protected isSessionExplicitlyTerminated (): boolean {
        return super.isSessionExplicitlyTerminated()
            || this.recentInputs.charCodeAt(this.recentInputs.length - 1) === 4
            || this.recentInputs.endsWith('exit\r')
    }

    @HostListener('click')
    onClick (): void {
        this.activeKIPrompt = null
    }
}
