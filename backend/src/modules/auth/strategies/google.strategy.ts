import { config } from "#@/config/config.js";
import type { AuthResult, OAuthTokenPayload } from "#@/modules/auth/types/index.js";
import { OAuth2Client } from "google-auth-library";
import createHttpError from "http-errors";
import { jwtService } from "#@/shared/utils/jwt-services.js";
import { extractEmail, extractStudentID } from "#@/modules/auth/utils/student-email.js"
import { UserFacade } from "#@/modules/user/user.facade.js"
import { type IAuthStrategy } from "./auth.strategy.js";

export class GoogleAuthStrategy implements IAuthStrategy {
    private readonly googleClient: OAuth2Client;
    constructor(
        private readonly userFacade: UserFacade
    ) {
        this.googleClient = new OAuth2Client(config.google.clientId)
    }
    /**
     * Verifies a Google ID token and extracts its payload.
     * @param idToken The Google ID token string.
     * @returns A promise resolving to the extracted Google token payload.
     * @throws Error if the payload is invalid or email is missing.
     */
    private async verifyGoogleToken(idToken: string): Promise<OAuthTokenPayload> {
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
            sub: payload?.sub
        }
    }
    /**
     * Checks if the given email belongs to an allowed domain.
     * @param email The email address to check.
     * @returns True if the domain is allowed, false otherwise.
     */
    private isAllowDomain(email: string): boolean {
        return config.allowedDomains.some((domain: string) => email.endsWith(domain));
    }

    /**
     * Finds an existing user by email or creates a new one if they don't exist.
     * Attempts to resolve student ID and name from the student directory if necessary.
     * @param googlePayload The payload extracted from the Google ID token.
     * @returns A promise resolving to the user's basic information.
     */
    private async getOrCreateUser(googlePayload: OAuthTokenPayload) {
        const foundUser = await this.userFacade.findByEmail(googlePayload.email);
        if (foundUser) {
            return {
                id: foundUser.id!.toString(),
                email: foundUser.email,
                name: foundUser.name,
                student_id: foundUser?.student_id,
                avatar_url: foundUser.avatar_url
            };
        }

        const emailProcessed = extractEmail(googlePayload.email);
        const extractedStudentId = extractStudentID(googlePayload.email);

        const newUserParams = {
            email: googlePayload.email,
            name: googlePayload.name,
            avatar_url: googlePayload.picture,
            student_id: extractedStudentId
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


    /**
     * Authenticates a user using a Google ID token.
     * Validates the token, checks the domain, creates/retrieves the user, and generates JWT tokens.
     * @param idToken The Google ID token to authenticate.
     * @returns A promise resolving to the authentication result containing tokens and user data.
     * @throws Unauthorized if the email domain is not allowed.
     */
    async authenticate(idToken: string): Promise<AuthResult> {
        const googlePayload = await this.verifyGoogleToken(idToken);
        if (!this.isAllowDomain(googlePayload.email)) {
            throw createHttpError.Unauthorized("Email domain not allowed")
        }

        const user = await this.getOrCreateUser(googlePayload);
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