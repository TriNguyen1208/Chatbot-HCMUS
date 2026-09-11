import type { User } from "./user.entity.js";
import { userContainer } from "./user.container.js";

export class UserFacade {
    private get userRepository() {
        return userContainer.userRepository;
    }
    async findByID(id: string) {
        return this.userRepository.findByID(id);
    }

    async findByEmail(email: string) {
        return this.userRepository.findByEmail(email);
    }

    async create(
        payload: {
            email: string;
            name: string;
            avatar_url?: string;
            student_id?: string;
            role?: string;
        }) {
        return this.userRepository.create(payload);
    }

    async update(id: string, payload: Partial<User>) {
        return this.userRepository.update(id, payload);
    }

    async updatePresence(userId: string, lastActive: Date): Promise<void> {
        return userContainer.userService.updatePresence(userId, lastActive);
    }
}

export const userFacade = new UserFacade()