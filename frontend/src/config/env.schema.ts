export interface ClientEnv {
  apiUrl: string;
  baseUrl: string;
  googleClientId?: string;
  microsoftClientId?: string;
  microsoftTenantId?: string;
  isProduction: boolean;
}

export const validateClientEnv = (): ClientEnv => {
  const apiUrl = process.env.NEXT_PUBLIC_API_URL || "http://localhost:5000";
  const baseUrl = process.env.NEXT_PUBLIC_BASE_URL || "http://localhost:3000";
  const googleClientId = process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID;
  const microsoftClientId = process.env.NEXT_PUBLIC_MICROSOFT_CLIENT_ID;
  const microsoftTenantId = process.env.NEXT_PUBLIC_MICROSOFT_TENANT_ID;
  const isProduction = process.env.NODE_ENV === "production";

  if (!microsoftClientId && !microsoftTenantId) {
    console.warn("⚠️ Warning: NEXT_PUBLIC_MICROSOFT_CLIENT_ID and NEXT_PUBLIC_MICROSOFT_TENANT_ID are not set.");
  }

  return {
    apiUrl,
    baseUrl,
    googleClientId,
    microsoftClientId,
    microsoftTenantId,
    isProduction,
  };
};
