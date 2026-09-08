import { mongoDB } from "#@/infrastructure/database/mongoDBAtlas.js";
import { UserRepository } from "./repositories/user.repository.js";
import { UserService } from "./services/user.service.js";
import { UserController } from "./controllers/user.controller.js";

class UserContainer {
    private _userRepository?: UserRepository;
    public get userRepository() {
        if (!this._userRepository) {
            this._userRepository = new UserRepository(mongoDB);
        }
        return this._userRepository;
    }
    
    private _userService?: UserService;
    public get userService() {
        if (!this._userService) {
            this._userService = new UserService(this.userRepository);
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
