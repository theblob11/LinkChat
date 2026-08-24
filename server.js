const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");
const { Pool } = require("pg");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

// Connect to Render PostgreSQL
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false
    }
});


// Serve website
app.use(express.static(path.join(__dirname, "public")));

app.use((req, res) => {
    res.sendFile(
        path.join(__dirname, "public", "index.html")
    );
});


// Create messages table
async function setupDatabase() {

    await pool.query(`
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            room TEXT NOT NULL,
            username TEXT NOT NULL,
            message TEXT NOT NULL,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    `);

    console.log("Database ready.");
}


// Store connected users
const rooms = new Map();


function send(ws, data) {

    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }

}


function broadcast(room, data) {

    const users = rooms.get(room);

    if (!users) return;

    for (const user of users) {
        send(user.ws, data);
    }

}


function broadcastExcept(room, data, exceptWs) {

    const users = rooms.get(room);

    if (!users) return;

    for (const user of users) {

        if (user.ws !== exceptWs) {
            send(user.ws, data);
        }

    }

}


// WebSocket connection
wss.on("connection", (ws) => {

    let currentRoom = null;
    let currentUser = null;


    ws.on("message", async (raw) => {

        let data;

        try {

            data = JSON.parse(
                raw.toString()
            );

        } catch {

            return;

        }


        // JOIN ROOM
        if (data.type === "join") {

            const room =
                String(data.room || "")
                .trim()
                .slice(0, 100);

            const username =
                String(data.username || "Anonymous")
                .trim()
                .slice(0, 30);


            if (!room) {

                send(ws, {
                    type: "error",
                    message: "Invalid room."
                });

                return;
            }


            currentRoom = room;
            currentUser = username;


            if (!rooms.has(room)) {
                rooms.set(room, new Set());
            }


            rooms.get(room).add({
                ws,
                username
            });


            // Tell user they joined
            send(ws, {
                type: "joined",
                room,
                username
            });


            // Load saved messages
            try {

                const result =
                    await pool.query(
                        `
                        SELECT
                            username,
                            message,
                            created_at
                        FROM messages
                        WHERE room = $1
                        ORDER BY id DESC
                        LIMIT 100
                        `,
                        [room]
                    );


                const history =
                    result.rows.reverse();


                for (const msg of history) {

                    send(ws, {
                        type: "message",
                        username: msg.username,
                        message: msg.message,
                        time: msg.created_at
                    });

                }

            } catch (error) {

                console.error(
                    "Could not load history:",
                    error
                );

            }


            // Tell everyone someone joined
            broadcastExcept(
                room,
                {
                    type: "system",
                    message:
                        `${username} joined the chat.`
                },
                ws
            );


            broadcast(room, {
                type: "users",
                count: rooms.get(room).size
            });


            return;
        }


        // SEND MESSAGE
        if (data.type === "message") {

            if (!currentRoom || !currentUser) {
                return;
            }


            const message =
                String(data.message || "")
                .trim()
                .slice(0, 1000);


            if (!message) {
                return;
            }


            try {

                // SAVE MESSAGE
                const result =
                    await pool.query(
                        `
                        INSERT INTO messages
                        (room, username, message)
                        VALUES ($1, $2, $3)
                        RETURNING created_at
                        `,
                        [
                            currentRoom,
                            currentUser,
                            message
                        ]
                    );


                const time =
                    result.rows[0].created_at;


                // Send message to everyone
                broadcast(
                    currentRoom,
                    {
                        type: "message",
                        username: currentUser,
                        message: message,
                        time: time
                    }
                );


            } catch (error) {

                console.error(
                    "Could not save message:",
                    error
                );


                send(ws, {
                    type: "error",
                    message:
                        "Message could not be saved."
                });

            }

        }

    });


    // USER DISCONNECTS
    ws.on("close", () => {

        if (!currentRoom) {
            return;
        }


        const users =
            rooms.get(currentRoom);


        if (!users) {
            return;
        }


        for (const user of users) {

            if (user.ws === ws) {

                users.delete(user);

                break;
            }

        }


        broadcast(
            currentRoom,
            {
                type: "system",
                message:
                    `${currentUser || "Someone"} left the chat.`
            }
        );


        if (users.size === 0) {

            rooms.delete(currentRoom);

        } else {

            broadcast(
                currentRoom,
                {
                    type: "users",
                    count: users.size
                }
            );

        }

    });

});


// Start server
async function startServer() {

    try {

        await setupDatabase();


        server.listen(
            PORT,
            "0.0.0.0",
            () => {

                console.log(
                    `Chat server running on port ${PORT}`
                );

            }
        );

    } catch (error) {

        console.error(
            "Database startup failed:",
            error
        );

        process.exit(1);

    }

}


startServer();
