import { createClient } from "@supabase/supabase-js";

async function test() {
    const supabase = createClient("https://mock.supabase.co", "dummy", {
        global: {
            fetch: (url, options) => {
                console.log("URL:", url);
                console.log("Method:", options.method);
                return Promise.resolve(new Response(JSON.stringify([]), { status: 200 }));
            }
        }
    });
    
    // Scenario 1: select then eq
    let q1 = supabase.from("users").update({ name: "test" }).select();
    q1 = q1.eq("id", "123");
    await q1;

    // Scenario 2: eq then select
    let q2 = supabase.from("users").update({ name: "test" }).eq("id", "123").select();
    await q2;
}
test();
