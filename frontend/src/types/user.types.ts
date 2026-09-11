export type UserRole = "student" | "lecturer" | "staff" | "admin";

export interface User {
  id: string;
  _id?: string;
  name: string;
  email: string;
  student_id?: string;
  studentID?: string; // Tương thích ngược
  role?: string;
  phone?: string;
  avatar_url?: string | null;
  is_online?: boolean;
  last_active?: Date | string;
  created_at?: Date | string;
}

export type UserProfile = User;

export interface TokenPair {
  accessToken: string;
  refreshToken: string;
}

export interface AuthResult {
  tokens: TokenPair;
  user: User;
}

export interface UpdateProfileDto {
  phone?: string;
  avatar_url?: string;
}
