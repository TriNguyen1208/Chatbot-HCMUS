import { AuthLeftPanel } from "@/features/auth/components/AuthLeftPanel";
import { AuthRightPanel } from "@/features/auth/components/AuthRightPanel";

export default function LoginPage() {
    return (
        <div className="min-h-screen flex flex-col lg:flex-row w-full">
            <AuthLeftPanel />
            <AuthRightPanel />
        </div>
    );
}
