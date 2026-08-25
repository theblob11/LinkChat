const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");
const crypto = require("crypto");
const { Pool } = require("pg");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 10000;


/* =========================
   DATABASE
========================= */

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: {
        rejectUnauthorized: false
    }
});


/* =========================
   WEBSITE
========================= */

app.use(
    express.static(
        path.join(__dirname, "public")
    )
);

app.use((req, res) => {
    res.sendFile(
        path.join(
            __dirname,
            "public",
            "index.html"
        )
    );
});


/* =========================
   DATABASE SETUP
========================= */

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

    await pool.query(`
        CREATE TABLE IF NOT EXISTS rooms (
            room TEXT PRIMARY KEY,
            password_hash TEXT
        )
    `);

    console.log("Database ready.");
}


/* =========================
   ONLINE ROOMS
========================= */

const rooms = new Map();


/* =========================
   SEND
========================= */

function send(ws, data) {

    if (
        ws.readyState ===
        WebSocket.OPEN
    ) {
        ws.send(
            JSON.stringify(data)
        );
    }
}


/* =========================
   ONLINE USER NAMES
========================= */

function getOnlineUsers(room) {

    const users = rooms.get(room);

    if (!users) {
        return [];
    }

    return Array.from(users).map(
        user => user.username
    );
}


/* =========================
   SEND ONLINE USERS
========================= */

function updateOnlineUsers(room) {

    const users = rooms.get(room);

    if (!users) {
        return;
    }

    const onlineUsers =
        getOnlineUsers(room);

    broadcast(room, {
        type: "users",
        count: onlineUsers.length,
        users: onlineUsers
    });
}


/* =========================
   BROADCAST
========================= */

function broadcast(room, data) {

    const users = rooms.get(room);

    if (!users) {
        return;
    }

    for (const user of users) {

        send(
            user.ws,
            data
        );

    }
}


/* =========================
   PASSWORD HASH
========================= */

function hashPassword(password) {

    return crypto
        .createHash("sha256")
        .update(password)
        .digest("hex");
}


/* =========================
   WEBSOCKET
========================= */

wss.on("connection", (ws) => {

    let currentRoom = null;
    let currentUser = null;


    ws.on(
        "message",
        async (raw) => {

            let data;

            try {

                data = JSON.parse(
                    raw.toString()
                );

            } catch {

                console.error(
                    "Invalid JSON received."
                );

                return;
            }


            /* =========================
               JOIN ROOM
            ========================= */

            if (
                data.type === "join"
            ) {

                const room =
                    String(
                        data.room || ""
                    )
                    .trim()
                    .slice(0, 100);

                const username =
                    String(
                        data.username ||
                        "Anonymous"
                    )
                    .trim()
                    .slice(0, 30);

                const password =
                    String(
                        data.password || ""
                    );


                if (!room) {

                    send(ws, {
                        type: "error",
                        message:
                            "Invalid room."
                    });

                    return;
                }


                if (!username) {

                    send(ws, {
                        type: "error",
                        message:
                            "Please enter a name."
                    });

                    return;
                }


                try {

                    const result =
                        await pool.query(
                            `
                            SELECT password_hash
                            FROM rooms
                            WHERE room = $1
                            `,
                            [room]
                        );


                    /* =========================
                       NEW ROOM
                    ========================= */

                    if (
                        result.rows.length === 0
                    ) {

                        const passwordHash =
                            password.length > 0
                                ? hashPassword(
                                    password
                                )
                                : null;


                        await pool.query(
                            `
                            INSERT INTO rooms
                            (room, password_hash)
                            VALUES ($1, $2)
                            `,
                            [
                                room,
                                passwordHash
                            ]
                        );

                    }


                    /* =========================
                       EXISTING ROOM
                    ========================= */

                    else {

                        const savedHash =
                            result.rows[0]
                                .password_hash;


                        if (savedHash) {

                            const suppliedHash =
                                hashPassword(
                                    password
                                );


                            if (
                                suppliedHash !==
                                savedHash
                            ) {

                                send(ws, {
                                    type:
                                        "passwordError",
                                    message:
                                        "Incorrect room password."
                                });

                                return;
                            }
                        }
                    }


                    /* =========================
                       ADD USER
                    ========================= */

                    currentRoom = room;
                    currentUser = username;


                    if (
                        !rooms.has(room)
                    ) {

                        rooms.set(
                            room,
                            new Set()
                        );

                    }


                    const user = {
                        ws: ws,
                        username:
                            username
                    };


                    rooms
                        .get(room)
                        .add(user);


                    send(ws, {
                        type: "joined",
                        room: room,
                        username:
                            username
                    });


                    /* =========================
                       LOAD HISTORY
                    ========================= */

                    const historyResult =
                        await pool.query(
                            `
                            SELECT
                                id,
                                username,
                                message,
                                created_at
                            FROM messages
                            WHERE room = $1
                            ORDER BY id ASC
                            LIMIT 100
                            `,
                            [room]
                        );


                    for (
                        const msg
                        of historyResult.rows
                    ) {

                        send(ws, {
                            type: "message",
                            id: msg.id,
                            username:
                                msg.username,
                            message:
                                msg.message,
                            time:
                                msg.created_at
                        });

                    }


                    /* =========================
                       JOIN NOTICE
                    ========================= */

                    broadcast(
                        room,
                        {
                            type: "system",
                            message:
                                `${username} joined the chat.`
                        }
                    );


                    /* =========================
                       ONLINE USERS
                    ========================= */

                    updateOnlineUsers(
                        room
                    );


                } catch (error) {

                    console.error(
                        "Join error:",
                        error
                    );

                    send(ws, {
                        type: "error",
                        message:
                            "Could not join the room."
                    });

                }

                return;
            }


            /* =========================
               SEND MESSAGE
            ========================= */

            if (
                data.type === "message"
            ) {

                if (
                    !currentRoom ||
                    !currentUser
                ) {
                    return;
                }


                const message =
                    String(
                        data.message || ""
                    )
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
                            type:
                                "message",
                            id:
                                saved.id,
                            username:
                                currentUser,
                            message:
                                message,
                            time:
                                saved.created_at
                        }
                    );


                } catch (error) {

                    console.error(
                        "Save message error:",
                        error
                    );

                }

                return;
            }


            /* =========================
               EDIT MESSAGE
            ========================= */

            if (
                data.type === "edit"
            ) {

                if (
                    !currentRoom ||
                    !currentUser
                ) {
                    return;
                }


                const messageId =
                    Number(data.id);


                const newMessage =
                    String(
                        data.message || ""
                    )
                    .trim()
                    .slice(0, 1000);


                if (
                    !Number.isInteger(
                        messageId
                    ) ||
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
                            `,
                            [
                                newMessage,
                                messageId,
                                currentRoom,
                                currentUser
                            ]
                        );


                    if (
                        result.rowCount === 0
                    ) {

                        send(ws, {
                            type:
                                "error",
                            message:
                                "You can only edit your own messages."
                        });

                        return;
                    }


                    broadcast(
                        currentRoom,
                        {
                            type:
                                "messageEdited",
                            id:
                                messageId,
                            message:
                                newMessage
                        }
                    );


                } catch (error) {

                    console.error(
                        "Edit error:",
                        error
                    );

                }

                return;
            }


            /* =========================
               DELETE MESSAGE
            ========================= */

            if (
                data.type === "delete"
            ) {

                if (
                    !currentRoom ||
                    !currentUser
                ) {
                    return;
                }


                const messageId =
                    Number(data.id);


                if (
                    !Number.isInteger(
                        messageId
                    )
                ) {
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
                            `,
                            [
                                messageId,
                                currentRoom,
                                currentUser
                            ]
                        );


                    if (
                        result.rowCount === 0
                    ) {

                        send(ws, {
                            type:
                                "error",
                            message:
                                "You can only delete your own messages."
                        });

                        return;
                    }


                    broadcast(
                        currentRoom,
                        {
                            type:
                                "messageDeleted",
                            id:
                                messageId
                        }
                    );


                } catch (error) {

                    console.error(
                        "Delete error:",
                        error
                    );

                }

                return;
            }

        }
    );


    /* =========================
       DISCONNECT
    ========================= */

    ws.on(
        "close",
        () => {

            if (!currentRoom) {
                return;
            }


            const users =
                rooms.get(
                    currentRoom
                );


            if (!users) {
                return;
            }


            for (
                const user of users
            ) {

                if (
                    user.ws === ws
                ) {

                    users.delete(
                        user
                    );

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


            if (
                users.size === 0
            ) {

                rooms.delete(
                    currentRoom
                );

            } else {

                updateOnlineUsers(
                    currentRoom
                );

            }

        }
    );

});


/* =========================
   START SERVER
========================= */

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
