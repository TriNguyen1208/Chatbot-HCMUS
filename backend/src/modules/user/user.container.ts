import { supabaseDB } from "#@/infrastructure/database/supabaseClient.js";
import { UserRepository } from "./repositories/user.repository.js";
import { UserService } from "./services/user.service.js";
import { UserController } from "./controllers/user.controller.js";

class UserContainer {
    public userRepository = new UserRepository(supabaseDB);
    public userService = new UserService(this.userRepository);
    public userController = new UserController(this.userService);
}

export const userContainer = new UserContainer();
