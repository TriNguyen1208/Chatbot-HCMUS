import { mongoDB } from "#@/infrastructure/database/mongoDBAtlas.js"
import { KeystoreService } from "#@/modules/auth/services/keystore.service.js"
import { KeyStoreRepository } from "#@/modules/auth/repositories/keystore.repository.js"
import { AuthController } from "#@/modules/auth/controllers/auth.controller.js"
import { AuthService } from "#@/modules/auth/services/auth.service.js"
import { MicrosoftAuthStrategy } from "#@/modules/auth/strategies/microsoft.strategy.js"
import { userFacade } from "#@/modules/user/user.facade.js"
import { GoogleAuthStrategy } from "#@/modules/auth/strategies/google.strategy.js"

class AuthContainer {
    public keystoreRepo = new KeyStoreRepository(mongoDB);

    public keystoreService = new KeystoreService(this.keystoreRepo);

    public authService = new AuthService(this.keystoreService);
    public authStrategy = new MicrosoftAuthStrategy(userFacade);
    public authGoogleStrategyTest = new GoogleAuthStrategy(userFacade);
    public authController = new AuthController(this.authService, this.authStrategy, this.keystoreService);
    public authGoogleControllerTest = new AuthController(this.authService, this.authGoogleStrategyTest, this.keystoreService);
}

export const authContainer = new AuthContainer();
