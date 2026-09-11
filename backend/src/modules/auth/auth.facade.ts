import { authContainer } from "./auth.container.js";

export class AuthFacade {
    private get authService() {
        return authContainer.authService;
    }

    private get keystoreService() {
        return authContainer.keystoreService;
    }

    async revokeAllUserSessions(userId: string): Promise<void> {
        return this.keystoreService.revokeAllByUser(userId);
    }
}

export const authFacade = new AuthFacade();
