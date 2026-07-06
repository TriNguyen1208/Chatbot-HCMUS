import { GoogleOAuthProvider } from "@react-oauth/google";
import { env } from "@/config/env";

export function Providers({ children }: { children: React.ReactNode }) {
  return (
    <GoogleOAuthProvider clientId={env.googleClientId}>
      <div className="text-gray-900 antialiased">{children}</div>
    </GoogleOAuthProvider>
  );
}