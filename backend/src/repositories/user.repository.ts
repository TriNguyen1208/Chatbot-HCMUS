import type { UserProfile } from "#@/types/index.js";
import { UserModel } from "#@/models/user.model.js";
import type { IUser } from "#@/models/user.model.js";

export interface IUserRepository{
    findByEmail(email: string): Promise<UserProfile | null>
    findByID(id: string): Promise<UserProfile | null>
    create({ email, name, avatarUrl, student_id }: { email: string; name: string; avatarUrl: string | undefined, student_id ?: string | undefined }) : Promise<UserProfile>;
}   

export class UserRepository implements IUserRepository{
    private toProfile(doc: IUser): UserProfile {
        return {
            id:        doc._id.toString(),
            email:     doc.email,
            name:      doc.name,
            student_id: doc.student_id,
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
    async create({ email, name, avatarUrl, student_id }: { email: string; name: string; avatarUrl: string; student_id?: string | undefined}): Promise<UserProfile> {
        const doc = await UserModel.create({email, name, avatarUrl, student_id})
        return this.toProfile(doc)
    }

}