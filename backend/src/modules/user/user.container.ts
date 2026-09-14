import { mongoDB } from "#@/infrastructure/database/mongodb.connection.js";
import { UserRepository } from "./user.repository.js";
import { UserService } from "./user.service.js";
import { UserController } from "./user.controller.js";
import { UserCache } from "./user.cache.js";

class UserContainer {
    private _userRepository?: UserRepository;
    public get userRepository() {
        if (!this._userRepository) {
            this._userRepository = new UserRepository(mongoDB);
        }
        return this._userRepository;
    }

    private _userCache?: UserCache;
    public get userCache() {
        if (!this._userCache) {
            this._userCache = new UserCache();
        }
        return this._userCache;
    }
    
    private _userService?: UserService;
    public get userService() {
        if (!this._userService) {
            this._userService = new UserService(this.userRepository, this.userCache);
        }
        return this._userService;
    }

    private _userController?: UserController;
    public get userController() {
        if (!this._userController) {
            this._userController = new UserController(this.userService);
        }
        return this._userController;
    }
}

export const userContainer = new UserContainer();
