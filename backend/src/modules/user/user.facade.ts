import type { User } from "./user.entity.js";
import { userContainer } from "./user.container.js";

export class UserFacade {
    private get userService() {
        return userContainer.userService;
    }

    async findByID(id: string): Promise<User | null> {
        return this.userService.findByID(id);
    }

    async findByEmail(email: string): Promise<User | null> {
        return this.userService.findByEmail(email);
    }

    async create(payload: {
        email: string;
        name: string;
        avatar_url?: string;
        student_id?: string;
        role?: string;
    }): Promise<User> {
        return this.userService.create(payload);
    }

    async update(id: string, payload: Partial<User>): Promise<User> {
        return this.userService.update(id, payload);
    }

    async updatePresence(userId: string, lastActive: Date): Promise<void> {
        return this.userService.updatePresence(userId, lastActive);
    }

    async getByID(userId: string): Promise<User> {
        return this.userService.getByID(userId);
    }

    async getBulk(ids: string[]): Promise<User[]> {
        return this.userService.getBulk(ids);
    }

    async setPresenceOnline(userId: string): Promise<void> {
        return this.userService.setPresenceOnline(userId);
    }

    async setPresenceOffline(userId: string, lastActive: Date): Promise<void> {
        return this.userService.setPresenceOffline(userId, lastActive);
    }
}

export const userFacade = new UserFacade();