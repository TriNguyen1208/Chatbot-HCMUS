import type { UserProfile } from "#@/types/index.js";
import { UserModel } from "#@/models/user.model.js";
import type { IUser } from "#@/models/user.model.js";

export interface IUserRepository{
    findByEmail(email: string): Promise<UserProfile | null>
    findByID(id: string): Promise<UserProfile | null>
    create({ email, name, avatarUrl, student_id }: { email: string; name: string; avatarUrl: string | undefined, student_id ?: string }) : Promise<UserProfile>;
}   

export class UserRepository implements IUserRepository{
    private toProfile(doc: IUser): UserProfile {
        return {
            id:        doc._id.toString(),
            email:     doc.email,
            name:      doc.name,
            studentID: doc.studentID,
            phone:     doc.phone,
            avatarUrl: doc.avatarUrl,
            createdAt: doc.createdAt,
            updatedAt: doc.updatedAt,
        };
    }
    async findByEmail(email: string): Promise<UserProfile | null> {
        const doc = await UserModel.findOne({email: email.toLowerCase()}).lean()
        return doc ? this.toProfile(doc) : null
    }
    async findByID(id: string): Promise<UserProfile | null> {
        const doc = await UserModel.findById(id).lean()
        return doc ? this.toProfile(doc) : null
    }
    async create({ email, name, avatarUrl }: { email: string; name: string; avatarUrl: string; }): Promise<UserProfile> {
        const doc = await UserModel.create({email, name, avatarUrl})
        return this.toProfile(doc)
    }

}