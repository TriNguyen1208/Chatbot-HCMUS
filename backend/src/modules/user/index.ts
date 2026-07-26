import { UserRepository } from "./repositories/user.repository.js";
import { UserFacade } from "./user.facade.js";
import { supabaseDB } from "#@/infrastructure/database/supabaseClient.js";

const userRepository = new UserRepository(supabaseDB);

// Khởi tạo instance và export ra ngoài cho toàn hệ thống dùng chung
export const userFacade = new UserFacade(userRepository);

export { default as authRoutes } from "#@/modules/user/routes/auth.routes.js";
