import { UserRepository } from "#@/modules/user/repositories/user.repository.js";

export class UserFacade {
    constructor(
        private readonly userRepository: UserRepository,
        // private readonly authService: AuthService // Nếu sau này cần dùng service
    ) {}
    
    //Cái nào cần public API cho các modules khác sử dụng thì viết ở đây
    //Tuyệt đối không được import repository hoặc là services
}
