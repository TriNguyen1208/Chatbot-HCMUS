import { config } from "#@/config/config.js";
import type { AuthResult, OAuthTokenPayload } from "#@/modules/auth/types/index.js";
import createHttpError from "http-errors";
import { jwtService } from "#@/shared/utils/jwt-services.js";
import { extractStudentID } from "#@/modules/auth/utils/student-email.js";
import { generateAvatarURI } from "#@/utils/avatar.util.js";
import { UserFacade } from "#@/modules/user/user.facade.js";
import { type IAuthStrategy } from "./auth.strategy.js";

export class MicrosoftAuthStrategy implements IAuthStrategy {
    constructor(
        private readonly userFacade: UserFacade
    ) { }

    /**
     * Verifies a Microsoft token (assumed to be an Access Token for Microsoft Graph).
     * @param token The Microsoft access token.
     */
    private async verifyMicrosoftToken(token: string): Promise<OAuthTokenPayload> {
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
        const email = data.mail || data.userPrincipalName;
        if (!email) throw new Error("Invalid Microsoft token payload: missing email");

        return {
            email: email,
            name: data.displayName || email,
            picture: "",
            sub: data.id
        }
    }

    private isAllowDomain(email: string): boolean {
        return config.allowedDomains.some((domain: string) => email.endsWith(domain));
    }

    private async getOrCreateUser(microsoftPayload: OAuthTokenPayload) {
        const foundUser = await this.userFacade.findByEmail(microsoftPayload.email);
        if (foundUser) {
            // Tự động sinh avatar nếu user cũ chưa có
            if (!foundUser.avatar_url) {
                const newAvatar = generateAvatarURI(foundUser.name);
                await this.userFacade.update(foundUser.id!.toString(), { avatar_url: newAvatar });
                foundUser.avatar_url = newAvatar;
            }

            return {
                id: foundUser.id!.toString(),
                email: foundUser.email,
                name: foundUser.name,
                student_id: foundUser?.student_id,
                avatar_url: foundUser.avatar_url
            };
        }

        const extractedStudentId = extractStudentID(microsoftPayload.email);

        const newUserParams: any = {
            email: microsoftPayload.email,
            name: microsoftPayload.name,
            student_id: extractedStudentId || "",
            avatar_url: generateAvatarURI(microsoftPayload.name)
        };

        const createdUser = await this.userFacade.create(newUserParams);

        return {
            id: createdUser.id!.toString(),
            email: newUserParams.email,
            name: newUserParams.name,
            student_id: newUserParams.student_id,
            avatar_url: newUserParams.avatar_url
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
                student_id: user.student_id,
                avatar_url: user.avatar_url
            }
        }
    }
}