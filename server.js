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

app.use(express.static(path.join(__dirname, "public")));

app.use((req, res) => {
    res.sendFile(
        path.join(__dirname, "public", "index.html")
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
            reply_to INTEGER,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    `);


    /*
       If the messages table already existed
       before reply_to was added, this makes sure
       the column exists.
    */

    await pool.query(`
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS reply_to INTEGER
    `);


    await pool.query(`
        CREATE TABLE IF NOT EXISTS rooms (
            room TEXT PRIMARY KEY,
            password_hash TEXT
        )
    `);


    /*
       One reaction per person per message.
    */

    await pool.query(`
        CREATE TABLE IF NOT EXISTS reactions (
            id SERIAL PRIMARY KEY,
            message_id INTEGER NOT NULL,
            username TEXT NOT NULL,
            reaction TEXT NOT NULL,

            UNIQUE(message_id, username)
        )
    `);


    console.log("Database ready.");
}


/* =========================
   ROOMS
========================= */

const rooms = new Map();


function send(ws, data) {

    if (ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify(data));
    }

}


function broadcast(room, data) {

    const users = rooms.get(room);

    if (!users) {
        return;
    }

    for (const user of users) {
        send(user.ws, data);
    }

}


function getOnlineUsers(room) {

    const users = rooms.get(room);

    if (!users) {
        return [];
    }

    return Array.from(users).map(
        user => user.username
    );

}


function updateOnlineUsers(room) {

    const users = rooms.get(room);

    if (!users) {
        return;
    }

    broadcast(room, {
        type: "users",
        count: users.size,
        users: getOnlineUsers(room)
    });

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
   VALID REACTIONS
========================= */

const allowedReactions = [
    "👍",
    "❤️",
    "😂",
    "😮",
    "😢",
    "😡"
];


/* =========================
   GET REACTIONS
========================= */

async function getReactions(messageId) {

    const result = await pool.query(
        `
        SELECT
            reaction,
            COUNT(*)::INTEGER AS count
        FROM reactions
        WHERE message_id = $1
        GROUP BY reaction
        `,
        [messageId]
    );


    const reactions = {};


    for (const row of result.rows) {

        reactions[row.reaction] =
            row.count;

    }


    return reactions;
}


/* =========================
   WEBSOCKET
========================= */

wss.on("connection", (ws) => {

    let currentRoom = null;
    let currentUser = null;


    ws.on("message", async (raw) => {

        let data;


        try {

            data =
                JSON.parse(
                    raw.toString()
                );

        } catch {

            send(ws, {
                type: "error",
                message: "Invalid request."
            });

            return;
        }


        /* =========================
           JOIN ROOM
        ========================= */

        if (data.type === "join") {

            const room =
                String(data.room || "")
                    .trim()
                    .slice(0, 100);


            const username =
                String(data.username || "Anonymous")
                    .trim()
                    .slice(0, 30);


            const password =
                String(data.password || "");


            if (!room) {

                send(ws, {
                    type: "error",
                    message: "Invalid room."
                });

                return;
            }


            if (!username) {

                send(ws, {
                    type: "error",
                    message: "Please enter a name."
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


                /*
                   Create room if it doesn't exist.
                */

                if (result.rows.length === 0) {

                    const passwordHash =
                        password.length > 0
                            ? hashPassword(password)
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

                } else {

                    /*
                       Existing room.
                    */

                    const savedHash =
                        result.rows[0].password_hash;


                    if (savedHash) {

                        const suppliedHash =
                            hashPassword(password);


                        if (
                            suppliedHash !==
                            savedHash
                        ) {

                            send(ws, {
                                type: "passwordError",
                                message:
                                    "Incorrect room password."
                            });

                            return;
                        }

                    }

                }


                currentRoom = room;
                currentUser = username;


                if (!rooms.has(room)) {
                    rooms.set(
                        room,
                        new Set()
                    );
                }


                const user = {
                    ws: ws,
                    username: username
                };


                rooms.get(room).add(user);


                send(ws, {
                    type: "joined",
                    room: room,
                    username: username
                });


                /* =========================
                   LOAD MESSAGE HISTORY
                ========================= */

                const history =
                    await pool.query(
                        `
                        SELECT
                            id,
                            username,
                            message,
                            reply_to,
                            created_at
                        FROM messages
                        WHERE room = $1
                        ORDER BY id ASC
                        LIMIT 100
                        `,
                        [room]
                    );


                for (const msg of history.rows) {

                    const reactions =
                        await getReactions(
                            msg.id
                        );


                    send(ws, {
                        type: "message",
                        id: msg.id,
                        username: msg.username,
                        message: msg.message,
                        replyTo: msg.reply_to,
                        time: msg.created_at,
                        reactions: reactions
                    });

                }


                broadcast(room, {
                    type: "system",
                    message:
                        `${username} joined the chat.`
                });


                updateOnlineUsers(room);

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
           TYPING
        ========================= */

        if (data.type === "typing") {

            if (
                !currentRoom ||
                !currentUser
            ) {
                return;
            }


            const users =
                rooms.get(currentRoom);


            if (!users) {
                return;
            }


            for (const user of users) {

                if (user.ws !== ws) {

                    send(user.ws, {
                        type: "typing",
                        username: currentUser,
                        typing:
                            Boolean(data.typing)
                    });

                }

            }


            return;
        }


        /* =========================
           SEND MESSAGE
        ========================= */

        if (data.type === "message") {

            if (
                !currentRoom ||
                !currentUser
            ) {
                return;
            }


            const message =
                String(data.message || "")
                    .trim()
                    .slice(0, 1000);


            if (!message) {
                return;
            }


            let replyTo = null;


            if (
                data.replyTo !== null &&
                data.replyTo !== undefined
            ) {

                const number =
                    Number(data.replyTo);


                if (
                    Number.isInteger(number) &&
                    number > 0
                ) {

                    /*
                       Make sure the replied-to
                       message belongs to this room.
                    */

                    const replyCheck =
                        await pool.query(
                            `
                            SELECT id
                            FROM messages
                            WHERE id = $1
                            AND room = $2
                            `,
                            [
                                number,
                                currentRoom
                            ]
                        );


                    if (
                        replyCheck.rows.length > 0
                    ) {

                        replyTo = number;

                    }

                }

            }


            try {

                const result =
                    await pool.query(
                        `
                        INSERT INTO messages
                        (
                            room,
                            username,
                            message,
                            reply_to
                        )
                        VALUES ($1, $2, $3, $4)
                        RETURNING
                            id,
                            created_at
                        `,
                        [
                            currentRoom,
                            currentUser,
                            message,
                            replyTo
                        ]
                    );


                const saved =
                    result.rows[0];


                broadcast(currentRoom, {
                    type: "message",
                    id: saved.id,
                    username: currentUser,
                    message: message,
                    replyTo: replyTo,
                    time: saved.created_at,
                    reactions: {}
                });


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

        if (data.type === "edit") {

            if (
                !currentRoom ||
                !currentUser
            ) {
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


                broadcast(currentRoom, {
                    type: "messageEdited",
                    id: messageId,
                    message: newMessage
                });


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

        if (data.type === "delete") {

            if (
                !currentRoom ||
                !currentUser
            ) {
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


                /*
                   Delete reactions belonging
                   to the deleted message.
                */

                await pool.query(
                    `
                    DELETE FROM reactions
                    WHERE message_id = $1
                    `,
                    [messageId]
                );


                broadcast(currentRoom, {
                    type: "messageDeleted",
                    id: messageId
                });


            } catch (error) {

                console.error(
                    "Delete error:",
                    error
                );

            }


            return;
        }


        /* =========================
           REACTION
        ========================= */

        if (data.type === "reaction") {

            if (
                !currentRoom ||
                !currentUser
            ) {
                return;
            }


            const messageId =
                Number(data.id);


            const reaction =
                String(data.reaction || "");


            if (
                !Number.isInteger(messageId) ||
                !allowedReactions.includes(
                    reaction
                )
            ) {
                return;
            }


            try {

                /*
                   Make sure message belongs
                   to current room.
                */

                const messageCheck =
                    await pool.query(
                        `
                        SELECT id
                        FROM messages
                        WHERE id = $1
                        AND room = $2
                        `,
                        [
                            messageId,
                            currentRoom
                        ]
                    );


                if (
                    messageCheck.rows.length === 0
                ) {
                    return;
                }


                /*
                   Check the user's current
                   reaction.
                */

                const existing =
                    await pool.query(
                        `
                        SELECT id, reaction
                        FROM reactions
                        WHERE message_id = $1
                        AND username = $2
                        `,
                        [
                            messageId,
                            currentUser
                        ]
                    );


                if (
                    existing.rows.length > 0
                ) {

                    const oldReaction =
                        existing.rows[0].reaction;


                    /*
                       Clicking the same reaction
                       removes it.
                    */

                    if (
                        oldReaction === reaction
                    ) {

                        await pool.query(
                            `
                            DELETE FROM reactions
                            WHERE message_id = $1
                            AND username = $2
                            `,
                            [
                                messageId,
                                currentUser
                            ]
                        );

                    } else {

                        /*
                           Change reaction.
                        */

                        await pool.query(
                            `
                            UPDATE reactions
                            SET reaction = $1
                            WHERE message_id = $2
                            AND username = $3
                            `,
                            [
                                reaction,
                                messageId,
                                currentUser
                            ]
                        );

                    }

                } else {

                    /*
                       Add new reaction.
                    */

                    await pool.query(
                        `
                        INSERT INTO reactions
                        (
                            message_id,
                            username,
                            reaction
                        )
                        VALUES ($1, $2, $3)
                        `,
                        [
                            messageId,
                            currentUser,
                            reaction
                        ]
                    );

                }


                const reactions =
                    await getReactions(
                        messageId
                    );


                /*
                   Tell everybody in the room
                   about the new reaction counts.
                */

                broadcast(currentRoom, {
                    type: "reactionUpdate",
                    id: messageId,
                    reactions: reactions
                });


            } catch (error) {

                console.error(
                    "Reaction error:",
                    error
                );

            }


            return;
        }

    });


    /* =========================
       DISCONNECT
    ========================= */

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


        /*
           Tell remaining users that
           this person stopped typing.
        */

        broadcast(currentRoom, {
            type: "typing",
            username: currentUser,
            typing: false
        });


        broadcast(currentRoom, {
            type: "system",
            message:
                `${currentUser || "Someone"} left the chat.`
        });


        if (users.size === 0) {

            rooms.delete(currentRoom);

        } else {

            updateOnlineUsers(currentRoom);

        }

    });

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
