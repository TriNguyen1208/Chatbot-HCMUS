import { config } from "#@/config/config.js";
import type { AuthResult, GoogleTokenPayload } from "#@/modules/user/types/index.js";
import { OAuth2Client } from "google-auth-library";
import createHttpError from "http-errors";
import type { IUserRepository } from "#@/modules/user/repositories/user.repository.js";
import { jwtService } from "#@/shared/utils/jwt-services.js";
import type { IStudentDirectoryRepository } from "#@/modules/user/repositories/student-directory.repository.js";
import { extractEmail, extractStudentID } from "#@/modules/user/utils/student-email.utils.js"

export interface IAuthStrategy {
    authenticate(credential: string): Promise<AuthResult>
}

export class GoogleAuthStrategy implements IAuthStrategy {
    private readonly googleClient: OAuth2Client;
    constructor(
        private readonly userRepository: IUserRepository,
        private readonly studentDirectoryRepository: IStudentDirectoryRepository,
    ) {
        this.googleClient = new OAuth2Client(config.google.clientId)
        this.userRepository = userRepository
        this.studentDirectoryRepository = studentDirectoryRepository
    }
    private async verifyGoogleToken(idToken: string): Promise<GoogleTokenPayload> {
        const ticket = await this.googleClient.verifyIdToken({
            idToken,
            audience: config.google.clientId
        })
        const payload = ticket.getPayload()
        if (!payload?.email) throw new Error("Invalid Google token payload");
        return {
            email: payload.email,
            name: payload.name || payload.email,
            picture: payload.picture,
            sub: payload.sub
        }
    }
    private isAllowDomain(email: string): boolean {
        return config.allowedDomains.some((domain: string) => email.endsWith(domain));
    }
    async authenticate(idToken: string): Promise<AuthResult> {
        const googlePayload = await this.verifyGoogleToken(idToken);
        if (!this.isAllowDomain(googlePayload.email)) {
            throw createHttpError.Unauthorized("Email domain not allowed")
        }

        const foundUser = await this.userRepository.findByEmail(googlePayload.email)
        let user;
        let userID;
        let studentID;
        if (foundUser) {
            user = {
                email: foundUser?.email,
                name: foundUser?.name,
                avatarUrl: googlePayload.picture,
                student_id: foundUser?.student_id,
            }
            userID = foundUser.id
            studentID = foundUser?.student_id
        }
        else {
            //Neu nhu chua dang nhap
            const email_processed = extractEmail(googlePayload.email)
            //TH1: Neu nhu trong gmail co student id thi extract ra luon
            const student_id = extractStudentID(googlePayload.email)
            if (!student_id) {
                //Tim user trong studentDirectory, neu tra ve lon hon 2 thi 
                const users = await this.studentDirectoryRepository.findByEmail(email_processed)
                if (users.length == 1) {
                    //Dam bao co thi create luon
                    user = {
                        email: googlePayload.email,
                        name: users[0]?.full_name ?? googlePayload.name,
                        avatar_url: googlePayload.picture,
                        student_id: users[0]?.student_id!
                    }
                    studentID = users[0]?.student_id!
                } else {
                    user = {
                        email: googlePayload.email,
                        name: googlePayload.name,
                        avatar_url: googlePayload.picture
                    }
                }
            } else {
                user = {
                    email: googlePayload.email,
                    name: googlePayload.name,
                    avatar_url: googlePayload.picture,
                    student_id: student_id
                }
                studentID = student_id
            }
            const createUser = await this.userRepository.create(user)
            userID = createUser.id
        }

        //Neu nhu user da ton tai ma phai dang nhap bang google => tuc la khong co access va refresh
        const tokens = jwtService.createPairToken({ id: userID, email: user.email! });
        return {
            tokens,
            user: {
                id: userID,
                email: user.email,
                name: user.name,
                student_id: studentID
            }
        }
    }
}