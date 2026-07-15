if(!process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID){
    throw new Error("Missing env: NEXT_PUBLIC_GOOGLE_CLIENT_ID");
}

export const env = {
    apiUrl: process.env.NEXT_PUBLIC_API_URL || "http://localhost:3001/api",
    googleClientId: process.env.NEXT_PUBLIC_GOOGLE_CLIENT_ID,
    isProduction: process.env.NODE_ENV === "production",
}