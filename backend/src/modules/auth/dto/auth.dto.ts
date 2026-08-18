import { z } from "zod";

export const GoogleLoginSchema = z.object({
    body: z.object({
        idToken: z
            .string({ message: "ID Token is required and must be a string" })
            .min(1, "Token ID cannot be empty"), 
    }).strict() //Compulsory to have
});

// Extract Type from Schema if needed to use at other levels
export type GoogleLoginInput = z.infer<typeof GoogleLoginSchema>;