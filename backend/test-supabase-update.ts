import { createClient } from "@supabase/supabase-js";

async function test() {
    const supabase = createClient("https://apudibqyboiyybgfjrie.supabase.co", process.env.SUPABASE_PUBLISHABLE_KEY || "dummy");
    try {
        let q = supabase.from("users").update({ name: "test" }).select();
        console.log("Has eq method?", typeof q.eq);
        q = q.eq("id", "123");
        console.log("Successfully chained eq after select");
    } catch(err) {
         console.error("Error:", err.message);
    }
}
test();
