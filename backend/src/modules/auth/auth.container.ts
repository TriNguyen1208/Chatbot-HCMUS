import { mongoDB } from "#@/infrastructure/database/mongodb.connection.js";
import { KeystoreService } from "./keystore.service.js";
import { KeyStoreRepository } from "./keystore.repository.js";
import { AuthController } from "./auth.controller.js";
import { AuthService } from "./auth.service.js";
import { MicrosoftAuthStrategy } from "./strategies/microsoft.strategy.js";
import { userFacade } from "#@/modules/user/user.facade.js";
import { GoogleAuthStrategy } from "./strategies/google.strategy.js";

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

    private _microsoftAuthStrategy?: MicrosoftAuthStrategy;
    public get microsoftAuthStrategy() {
        if (!this._microsoftAuthStrategy) {
            this._microsoftAuthStrategy = new MicrosoftAuthStrategy(userFacade);
        }
        return this._microsoftAuthStrategy;
    }
    public get authStrategy() { return this.microsoftAuthStrategy; }

    private _googleAuthStrategy?: GoogleAuthStrategy;
    public get googleAuthStrategy() {
        if (!this._googleAuthStrategy) {
            this._googleAuthStrategy = new GoogleAuthStrategy(userFacade);
        }
        return this._googleAuthStrategy;
    }
    public get authGoogleStrategyTest() { return this.googleAuthStrategy; }

    private _microsoftAuthController?: AuthController;
    public get microsoftAuthController() {
        if (!this._microsoftAuthController) {
            this._microsoftAuthController = new AuthController(this.authService, this.microsoftAuthStrategy, this.keystoreService);
        }
        return this._microsoftAuthController;
    }
    public get authController() { return this.microsoftAuthController; }

    private _googleAuthController?: AuthController;
    public get googleAuthController() {
        if (!this._googleAuthController) {
            this._googleAuthController = new AuthController(this.authService, this.googleAuthStrategy, this.keystoreService);
        }
        return this._googleAuthController;
    }
    public get authGoogleControllerTest() { return this.googleAuthController; }
}

export const authContainer = new AuthContainer();
