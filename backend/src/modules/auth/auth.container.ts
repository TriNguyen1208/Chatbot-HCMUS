import { mongoDB } from "#@/infrastructure/database/mongoDBAtlas.js"
import { KeystoreService } from "#@/modules/auth/services/keystore.service.js"
import { KeyStoreRepository } from "#@/modules/auth/repositories/keystore.repository.js"
import { AuthController } from "#@/modules/auth/controllers/auth.controller.js"
import { AuthService } from "#@/modules/auth/services/auth.service.js"
import { MicrosoftAuthStrategy } from "#@/modules/auth/strategies/microsoft.strategy.js"
import { userFacade } from "#@/modules/user/user.facade.js"
import { GoogleAuthStrategy } from "#@/modules/auth/strategies/google.strategy.js"

class AuthContainer {
    private _keystoreRepo?: KeyStoreRepository;
    public get keystoreRepo() {
        if (!this._keystoreRepo) {
            this._keystoreRepo = new KeyStoreRepository(mongoDB);
        }
        return this._keystoreRepo;
    }
    
    private _keystoreService?: KeystoreService;
    public get keystoreService() {
        if (!this._keystoreService) {
            this._keystoreService = new KeystoreService(this.keystoreRepo);
        }
        return this._keystoreService;
    }

    private _authService?: AuthService;
    public get authService() {
        if (!this._authService) {
            this._authService = new AuthService(this.keystoreService);
        }
        return this._authService;
    }

    private _authStrategy?: MicrosoftAuthStrategy;
    public get authStrategy() {
        if (!this._authStrategy) {
            this._authStrategy = new MicrosoftAuthStrategy(userFacade);
        }
        return this._authStrategy;
    }

    private _authGoogleStrategyTest?: GoogleAuthStrategy;
    public get authGoogleStrategyTest() {
        if (!this._authGoogleStrategyTest) {
            this._authGoogleStrategyTest = new GoogleAuthStrategy(userFacade);
        }
        return this._authGoogleStrategyTest;
    }

    private _authController?: AuthController;
    public get authController() {
        if (!this._authController) {
            this._authController = new AuthController(this.authService, this.authStrategy, this.keystoreService);
        }
        return this._authController;
    }

    private _authGoogleControllerTest?: AuthController;
    public get authGoogleControllerTest() {
        if (!this._authGoogleControllerTest) {
            this._authGoogleControllerTest = new AuthController(this.authService, this.authGoogleStrategyTest, this.keystoreService);
        }
        return this._authGoogleControllerTest;
    }
}

export const authContainer = new AuthContainer();
