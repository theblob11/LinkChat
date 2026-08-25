const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");
const { Pool } = require("pg");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false
    }
});

app.use(express.static(path.join(__dirname, "public")));

app.use((req, res) => {
    res.sendFile(
        path.join(__dirname, "public", "index.html")
    );
});


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


wss.on("connection", (ws) => {

    let currentRoom = null;
    let currentUser = null;


    ws.on("message", async (raw) => {

        let data;

        try {

            data = JSON.parse(raw.toString());

        } catch {

            return;

        }


        // JOIN
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
                            id,
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
                        id: msg.id,
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

                const result =
                    await pool.query(
                        `
                        INSERT INTO messages
                        (room, username, message)
                        VALUES ($1, $2, $3)
                        RETURNING id, created_at
                        `,
                        [
                            currentRoom,
                            currentUser,
                            message
                        ]
                    );


                const saved =
                    result.rows[0];


                broadcast(
                    currentRoom,
                    {
                        type: "message",
                        id: saved.id,
                        username: currentUser,
                        message: message,
                        time: saved.created_at
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

            return;
        }


        // EDIT MESSAGE
        if (data.type === "edit") {

            if (!currentRoom || !currentUser) {
                return;
            }


            const messageId =
                Number(data.id);


            const newMessage =
                String(data.message || "")
                .trim()
                .slice(0, 1000);


            if (
                !Number.isInteger(messageId) ||
                !newMessage
            ) {
                return;
            }


            try {

                const result =
                    await pool.query(
                        `
                        UPDATE messages
                        SET message = $1
                        WHERE id = $2
                        AND room = $3
                        AND username = $4
                        RETURNING id, message
                        `,
                        [
                            newMessage,
                            messageId,
                            currentRoom,
                            currentUser
                        ]
                    );


                if (result.rowCount === 0) {

                    send(ws, {
                        type: "error",
                        message:
                            "You can only edit your own messages."
                    });

                    return;
                }


                broadcast(
                    currentRoom,
                    {
                        type: "messageEdited",
                        id: messageId,
                        message: newMessage
                    }
                );


            } catch (error) {

                console.error(
                    "Could not edit message:",
                    error
                );

            }

            return;
        }


        // DELETE MESSAGE
        if (data.type === "delete") {

            if (!currentRoom || !currentUser) {
                return;
            }


            const messageId =
                Number(data.id);


            if (!Number.isInteger(messageId)) {
                return;
            }


            try {

                const result =
                    await pool.query(
                        `
                        DELETE FROM messages
                        WHERE id = $1
                        AND room = $2
                        AND username = $3
                        RETURNING id
                        `,
                        [
                            messageId,
                            currentRoom,
                            currentUser
                        ]
                    );


                if (result.rowCount === 0) {

                    send(ws, {
                        type: "error",
                        message:
                            "You can only delete your own messages."
                    });

                    return;
                }


                broadcast(
                    currentRoom,
                    {
                        type: "messageDeleted",
                        id: messageId
                    }
                );


            } catch (error) {

                console.error(
                    "Could not delete message:",
                    error
                );

            }

            return;
        }

    });


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
