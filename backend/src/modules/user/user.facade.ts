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
            student_id?: string
        }) {
        return this.userRepository.create(payload);
    }
}

export const userFacade = new UserFacade()