import { config } from "#@/config/config.js";
import type { AuthResult, GoogleTokenPayload } from "#@/modules/auth/types/index.js";
import createHttpError from "http-errors";
import { jwtService } from "#@/shared/utils/jwt-services.js";
import type { IStudentDirectoryRepository } from "#@/modules/auth/repositories/student_directory.repository.js";
import { extractStudentID } from "#@/modules/auth/utils/student-email.js"
import { UserFacade } from "#@/modules/user/user.facade.js"
import { type IAuthStrategy } from "./auth.strategy.js";

export class MicrosoftAuthStrategy implements IAuthStrategy {
    constructor(
        private readonly userFacade: UserFacade,
        private readonly studentDirectoryRepo: IStudentDirectoryRepository,
    ) { }

    /**
     * Verifies a Microsoft token (assumed to be an Access Token for Microsoft Graph).
     * @param token The Microsoft access token.
     */
    private async verifyMicrosoftToken(token: string): Promise<GoogleTokenPayload> {
        // Ghi chú: Nếu front-end gửi Access Token của Microsoft, ta có thể dùng fetch gọi lên Graph API
        // Nếu front-end gửi ID Token, ta sẽ cần config jwt-decode hoặc thư viện xác thực Azure AD.
        // Ở đây code giả định fetch từ Graph API bằng Access Token:
        const response = await fetch("https://graph.microsoft.com/v1.0/me", {
            headers: {
                Authorization: `Bearer ${token}`
            }
        });

        if (!response.ok) {
            // Log fallback in case it's actually an ID token and decode is needed
            console.error("Microsoft Graph API error:", await response.text());
            throw new Error("Invalid Microsoft token");
        }

        const data = await response.json();
        console.log(data)
        const email = data.mail || data.userPrincipalName;
        if (!email) throw new Error("Invalid Microsoft token payload: missing email");

        return {
            email: email,
            name: data.displayName || email,
            picture: "", // Graph /me không trả về avatar trực tiếp, cần call /me/photo/$value
            sub: data.id
        }
    }

    private isAllowDomain(email: string): boolean {
        return config.allowedDomains.some((domain: string) => email.endsWith(domain));
    }

    private async getOrCreateUser(microsoftPayload: any) {
        // Lấy mail từ payload nếu như tìm thấy trong database thì trả về luôn (tức là đã từng đăng nhập rồi)
        const foundUser = await this.userFacade.findByEmail(microsoftPayload.email);
        if (foundUser) {
            return {
                id: foundUser.id,
                email: foundUser.email,
                name: foundUser.name,
                student_id: foundUser?.student_id
            };
        }

        // Nếu như chưa từng đăng nhập thì lấy studentID từ payload của mail
        const extractedStudentId = extractStudentID(microsoftPayload.email);

        const newUserParams = {
            email: microsoftPayload.email,
            name: microsoftPayload.name,
            avatar_url: microsoftPayload.picture,
            student_id: extractedStudentId || ""
        };

        if (extractedStudentId) {
            // Dựa vào MSSV này thì tìm trong studentDirectoryRepo, findByStudentID
            const users = await this.studentDirectoryRepo.findByStudentID(extractedStudentId);

            if (users && users.length > 0) {
                // Nếu như tìm được thì trả về (name và studentID)
                newUserParams.name = users[0]!.full_name || microsoftPayload.name;
                newUserParams.student_id = users[0]!.student_id;
            } else {
                // Nếu như không tìm được thì lấy studentID làm name và studentID làm studentID
                newUserParams.name = extractedStudentId;
                newUserParams.student_id = extractedStudentId;
            }
        }

        const createdUser = await this.userFacade.create(newUserParams);

        return {
            id: createdUser.id,
            email: newUserParams.email,
            name: newUserParams.name,
            student_id: newUserParams.student_id
        };
    }

    async authenticate(token: string): Promise<AuthResult> {
        const microsoftPayload = await this.verifyMicrosoftToken(token);

        if (!this.isAllowDomain(microsoftPayload.email)) {
            throw createHttpError.Unauthorized("Email domain not allowed")
        }

        const user = await this.getOrCreateUser(microsoftPayload);
        const tokens = jwtService.createPairToken({ id: user.id, email: user.email! });

        return {
            tokens,
            user: {
                id: user.id,
                email: user.email,
                name: user.name,
                student_id: user.student_id
            }
        }
    }
}