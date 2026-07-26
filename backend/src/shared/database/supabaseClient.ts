import { type IDatabase, type IDatabaseUserService } from "./database.interface.js";
import { createClient, SupabaseClient } from "@supabase/supabase-js";

export class SupabaseDatabase implements IDatabase {
  private supabase: SupabaseClient | undefined;
  private connected: boolean = true;
  private readonly supabaseURL: string;
  private readonly supabasePublishableKey: string;

  constructor(url: string, publishableKey: string) {
    this.supabaseURL = url;
    this.supabasePublishableKey = publishableKey;
  }

  async connect(): Promise<void> {
    this.supabase = createClient(this.supabaseURL, this.supabasePublishableKey);
    const { error } = await this.supabase.from("users").select("*").limit(1);

    if (error && error.code !== "42P01" && error.code !== "PGRST116") {
      throw new Error(`Supabase connection failed: ${error.message}`);
    }

    this.connected = true;
  }

  async disconnect(): Promise<void> {
    await this.supabase?.removeAllChannels();
  }

  isConnected(): boolean {
    return (this.supabase !== undefined && this.connected);
  }
}

// export class UserService extends SupabaseDatabase implements IDatabaseUserService {
//   async findUserNames(): Promise<string[]> {
    
//   }
// }