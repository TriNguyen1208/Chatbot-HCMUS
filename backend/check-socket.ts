import { Server } from "socket.io";
import { createAdapter } from "@socket.io/redis-adapter";
import Redis from "ioredis";

async function main() {
    const pubClient = new Redis("redis://localhost:6379");
    const subClient = pubClient.duplicate();

    const io = new Server({
        adapter: createAdapter(pubClient, subClient)
    });

    const room = "user:6a8ad6fcab107d76f97fdabd";
    const sockets = await io.in(room).fetchSockets();

    console.log(`Number of sockets in room ${room}:`, sockets.length);
    sockets.forEach(s => console.log(s.id));
    
    process.exit(0);
}
main();
