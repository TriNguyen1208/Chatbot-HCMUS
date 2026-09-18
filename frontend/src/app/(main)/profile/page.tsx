import { cookies } from "next/headers";
import { ProfileForm } from "@/features/profile/components/ProfileForm";
import { env } from "@/config/env";
import type { User } from "@/types";

async function getProfile(): Promise<User | null> {
  try {
    const cookieStore = await cookies();
    const cookieHeader = cookieStore.toString();
    if (!cookieHeader) return null;

    const res = await fetch(`${env.apiUrl}/api/user/me`, {
      headers: {
        Cookie: cookieHeader,
      },
      cache: "no-store",
    });

    if (!res.ok) return null;
    const json = await res.json();
    return json?.data || null;
  } catch {
    return null;
  }
}

export default async function ProfilePage() {
  const user = await getProfile();
  return <ProfileForm initialUser={user || undefined} />;
}
