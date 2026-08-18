if (!process.env.NEXT_PUBLIC_MICROSOFT_CLIENT_ID && !process.env.NEXT_PUBLIC_MICROSOFT_TENANT_ID) {
    throw new Error("Missing env: NEXT_PUBLIC_MICROSOFT_CLIENT_ID and NEXT_PUBLIC_MICROSOFT_TENANT_ID");
}

export const env = {
    apiUrl: process.env.NEXT_PUBLIC_API_URL || "http://localhost:3001",
    baseUrl: process.env.NEXT_PUBLIC_BASE_URL || "http://localhost:3000",
    googleClientId: process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID,
    microsoftClientId: process.env.NEXT_PUBLIC_MICROSOFT_CLIENT_ID,
    microsoftTenantId: process.env.NEXT_PUBLIC_MICROSOFT_TENANT_ID,
    isProduction: process.env.NODE_ENV === "production",
}