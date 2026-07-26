import { supabaseDB } from "./src/infrastructure/database/supabaseClient.js";

async function test() {
    try {
        await supabaseDB.connect();
        const data = {
            user_id: '6713b241-2574-41e4-8c8b-7ea07d64ecee',
            refresh_token_hash: '87c6827f7a818cebd65fa9bc1d452e16331aee11b850cbce99677e054b065639',
            family_id: '449ffba8-f38b-4d08-acea-f7295e258d2d',
            parent_id: null,
            is_used: false,
            device_info: {
                user_agent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
                ip: '::1'
            },
            expires_at: new Date("2026-08-03T12:54:44.484Z")
        };
        const res = await supabaseDB.insert("keystores", data);
        console.log("Success:", res);
    } catch (err) {
        console.error("Error:", err);
    }
}
test();
