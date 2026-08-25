const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const { Pool } = require("pg");
const path = require("path");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);

const PORT = process.env.PORT || 10000;

/* =========================================================
   DATABASE
========================================================= */

if (!process.env.DATABASE_URL) {

    console.error(
        "DATABASE_URL is not set."
    );

    process.exit(1);
}

const pool = new Pool({
    connectionString:
        process.env.DATABASE_URL,

    ssl: {
        rejectUnauthorized: false
    }
});

/* =========================================================
   EXPRESS
========================================================= */

app.use(express.json());

app.use(
    express.static(
        path.join(
            __dirname,
            "public"
        )
    )
);

app.use(
    (req, res) => {

        res.sendFile(
            path.join(
                __dirname,
                "public",
                "index.html"
            )
        );
    }
);

/* =========================================================
   WEBSOCKET
========================================================= */

const wss =
    new WebSocket.Server({
        server
    });

const rooms =
    new Map();

/* =========================================================
   DATABASE SETUP
========================================================= */

async function setupDatabase() {

    console.log(
        "Checking database..."
    );

    /* =====================================================
       ROOMS
    ===================================================== */

    await pool.query(`
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            room TEXT UNIQUE NOT NULL,
            password TEXT DEFAULT '',
            owner TEXT DEFAULT '',
            locked BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    await pool.query(`
        ALTER TABLE rooms
        ADD COLUMN IF NOT EXISTS password
        TEXT DEFAULT ''
    `);

    await pool.query(`
        ALTER TABLE rooms
        ADD COLUMN IF NOT EXISTS owner
        TEXT DEFAULT ''
    `);

    await pool.query(`
        ALTER TABLE rooms
        ADD COLUMN IF NOT EXISTS locked
        BOOLEAN DEFAULT FALSE
    `);

    await pool.query(`
        ALTER TABLE rooms
        ADD COLUMN IF NOT EXISTS created_at
        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    `);

    /* =====================================================
       MESSAGES
    ===================================================== */

    await pool.query(`
        CREATE TABLE IF NOT EXISTS messages (
            id BIGSERIAL PRIMARY KEY,
            room TEXT NOT NULL,
            username TEXT NOT NULL,
            message TEXT NOT NULL,
            time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            edited BOOLEAN DEFAULT FALSE,
            deleted BOOLEAN DEFAULT FALSE,
            reply_to BIGINT
        )
    `);

    await pool.query(`
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS room
        TEXT
    `);

    await pool.query(`
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS username
        TEXT
    `);

    await pool.query(`
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS message
        TEXT
    `);

    await pool.query(`
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS time
        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    `);

    await pool.query(`
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS edited
        BOOLEAN DEFAULT FALSE
    `);

    await pool.query(`
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS deleted
        BOOLEAN DEFAULT FALSE
    `);

    await pool.query(`
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS reply_to
        BIGINT
    `);

    /* =====================================================
       REACTIONS
    ===================================================== */

    await pool.query(`
        CREATE TABLE IF NOT EXISTS reactions (
            message_id BIGINT NOT NULL,
            username TEXT NOT NULL,
            reaction TEXT NOT NULL,

            PRIMARY KEY (
                message_id,
                username,
                reaction
            )
        )
    `);

    /* =====================================================
       JJCE OWNER
    ===================================================== */

    await pool.query(`
        UPDATE rooms
        SET owner = 'Justin'
        WHERE room = 'JJCE'
    `);

    console.log(
        "Database ready."
    );
}

/* =========================================================
   HELPERS
========================================================= */

function cleanText(
    value,
    maxLength
) {

    if (
        typeof value !==
        "string"
    ) {

        return "";
    }

    return value
        .trim()
        .slice(
            0,
            maxLength
        );
}

function send(
    ws,
    data
) {

    if (
        ws &&
        ws.readyState ===
        WebSocket.OPEN
    ) {

        ws.send(
            JSON.stringify(
                data
            )
        );
    }
}

function broadcast(
    roomName,
    data,
    except = null
) {

    const room =
        rooms.get(
            roomName
        );

    if (!room) {
        return;
    }

    for (
        const client
        of room.users
    ) {

        if (
            client ===
            except
        ) {

            continue;
        }

        send(
            client,
            data
        );
    }
}

function getRoomUsers(
    roomName
) {

    const room =
        rooms.get(
            roomName
        );

    if (!room) {
        return [];
    }

    return Array
        .from(
            room.users
        )
        .filter(
            client =>
                client.username
        )
        .map(
            client =>
                client.username
        );
}

function updateUsers(
    roomName
) {

    broadcast(
        roomName,
        {
            type:
                "users",

            users:
                getRoomUsers(
                    roomName
                )
        }
    );
}

function hashPassword(
    password
) {

    if (!password) {
        return "";
    }

    return crypto
        .createHash(
            "sha256"
        )
        .update(
            password
        )
        .digest(
            "hex"
        );
}

/* =========================================================
   ROOM DATABASE
========================================================= */

async function getRoom(
    roomName
) {

    const result =
        await pool.query(
            `
            SELECT
                room,
                password,
                owner,
                locked
            FROM rooms
            WHERE room = $1
            `,
            [
                roomName
            ]
        );

    if (
        result.rows.length ===
        0
    ) {

        return null;
    }

    return result.rows[0];
}

async function createRoom(
    roomName,
    password,
    owner
) {

    const result =
        await pool.query(
            `
            INSERT INTO rooms
                (
                    room,
                    password,
                    owner,
                    locked
                )
            VALUES
                (
                    $1,
                    $2,
                    $3,
                    FALSE
                )
            RETURNING
                room,
                password,
                owner,
                locked
            `,
            [
                roomName,
                hashPassword(
                    password
                ),
                owner
            ]
        );

    return result.rows[0];
}

/* =========================================================
   ROOM SETTINGS
========================================================= */

function sendRoomSettings(
    roomName
) {

    const room =
        rooms.get(
            roomName
        );

    if (!room) {
        return;
    }

    broadcast(
        roomName,
        {
            type:
                "roomSettings",

            room:
                room.name,

            owner:
                room.owner,

            locked:
                room.locked,

            hasPassword:
                room.hasPassword
        }
    );
}

/* =========================================================
   MESSAGE HISTORY
========================================================= */

async function loadMessages(
    roomName,
    ws
) {

    const result =
        await pool.query(
            `
            SELECT
                m.id,
                m.username,
                m.message,
                m.time,
                m.edited,
                m.deleted,
                m.reply_to,

                COALESCE(
                    (
                        SELECT
                            json_object_agg(
                                reaction_counts.reaction,
                                reaction_counts.count
                            )
                        FROM (
                            SELECT
                                r.reaction,
                                COUNT(*)::int
                                    AS count

                            FROM reactions r

                            WHERE
                                r.message_id =
                                m.id

                            GROUP BY
                                r.reaction
                        )
                        reaction_counts
                    ),
                    '{}'::json
                )
                AS reactions

            FROM messages m

            WHERE
                m.room = $1

            ORDER BY
                m.id ASC

            LIMIT 500
            `,
            [
                roomName
            ]
        );

    for (
        const row
        of result.rows
    ) {

        send(
            ws,
            {
                type:
                    "message",

                id:
                    row.id,

                username:
                    row.username,

                message:
                    row.deleted
                        ? "[message deleted]"
                        : row.message,

                time:
                    row.time,

                edited:
                    row.edited,

                replyTo:
                    row.reply_to,

                reactions:
                    row.reactions ||
                    {}
            }
        );
    }
}

/* =========================================================
   REACTIONS
========================================================= */

async function getReactions(
    messageId
) {

    const result =
        await pool.query(
            `
            SELECT
                reaction,
                COUNT(*)::int AS count

            FROM reactions

            WHERE
                message_id = $1

            GROUP BY
                reaction
            `,
            [
                messageId
            ]
        );

    const reactions = {};

    for (
        const row
        of result.rows
    ) {

        reactions[
            row.reaction
        ] =
            row.count;
    }

    return reactions;
}

/* =========================================================
   JOIN ROOM
========================================================= */

async function joinRoom(
    ws,
    roomName,
    username,
    password
) {

    let databaseRoom =
        await getRoom(
            roomName
        );

    /* =====================================================
       CREATE ROOM
    ===================================================== */

    if (!databaseRoom) {

        databaseRoom =
            await createRoom(
                roomName,
                password,
                username
            );
    }

    /* =====================================================
       EXISTING ROOM
    ===================================================== */

    else {

        /* JJCE always belongs to Justin */

        if (
            roomName === "JJCE"
        ) {

            await pool.query(
                `
                UPDATE rooms
                SET owner = $1
                WHERE room = $2
                `,
                [
                    "Justin",
                    "JJCE"
                ]
            );

            databaseRoom.owner =
                "Justin";
        }

        /* =================================================
           LOCK CHECK
        ================================================= */

        if (
            databaseRoom.locked &&
            databaseRoom.owner
                .toLowerCase() !==
            username.toLowerCase()
        ) {

            send(
                ws,
                {
                    type:
                        "roomLocked",

                    message:
                        "This room is locked. The owner must unlock it before new users can join."
                }
            );

            return false;
        }

        /* =================================================
           PASSWORD CHECK
        ================================================= */

        const suppliedPassword =
            hashPassword(
                password
            );

        if (
            databaseRoom.password !==
            suppliedPassword
        ) {

            send(
                ws,
                {
                    type:
                        "passwordError",

                    message:
                        "Incorrect room password."
                }
            );

            return false;
        }
    }

    /* =====================================================
       DUPLICATE USERNAME
    ===================================================== */

    const currentRoom =
        rooms.get(
            roomName
        );

    if (currentRoom) {

        for (
            const client
            of currentRoom.users
        ) {

            if (
                client.username &&
                client.username
                    .toLowerCase() ===
                username.toLowerCase()
            ) {

                send(
                    ws,
                    {
                        type:
                            "error",

                        message:
                            "That username is already being used in this room."
                    }
                );

                return false;
            }
        }
    }

    /* =====================================================
       MEMORY ROOM
    ===================================================== */

    if (
        !rooms.has(
            roomName
        )
    ) {

        rooms.set(
            roomName,
            {
                name:
                    databaseRoom.room,

                owner:
                    databaseRoom.owner,

                locked:
                    Boolean(
                        databaseRoom.locked
                    ),

                hasPassword:
                    Boolean(
                        databaseRoom.password
                    ),

                users:
                    new Set()
            }
        );
    }

    const room =
        rooms.get(
            roomName
        );

    room.name =
        databaseRoom.room;

    room.owner =
        databaseRoom.owner;

    room.locked =
        Boolean(
            databaseRoom.locked
        );

    room.hasPassword =
        Boolean(
            databaseRoom.password
        );

    ws.room =
        roomName;

    ws.username =
        username;

    ws.isOwner =
        username.toLowerCase() ===
        room.owner.toLowerCase();

    room.users.add(
        ws
    );

    /* =====================================================
       JOIN RESPONSE
    ===================================================== */

    send(
        ws,
        {
            type:
                "joined",

            room:
                room.name,

            owner:
                room.owner,

            locked:
                room.locked,

            hasPassword:
                room.hasPassword,

            isOwner:
                ws.isOwner
        }
    );

    /* =====================================================
       HISTORY
    ===================================================== */

    await loadMessages(
        roomName,
        ws
    );

    /* =====================================================
       JOIN MESSAGE
    ===================================================== */

    broadcast(
        roomName,
        {
            type:
                "system",

            message:
                `${username} joined the room.`
        },
        ws
    );

    updateUsers(
        roomName
    );

    sendRoomSettings(
        roomName
    );

    return true;
}

/* =========================================================
   WEBSOCKET
========================================================= */

wss.on(
    "connection",
    ws => {

        ws.room =
            null;

        ws.username =
            null;

        ws.isOwner =
            false;

        send(
            ws,
            {
                type:
                    "connected"
            }
        );

        ws.on(
            "message",
            async raw => {

                let data;

                /* =================================================
                   PARSE
                ================================================= */

                try {

                    data =
                        JSON.parse(
                            raw.toString()
                        );

                } catch {

                    send(
                        ws,
                        {
                            type:
                                "error",

                            message:
                                "Invalid request."
                        }
                    );

                    return;
                }

                try {

                    /* =================================================
                       JOIN
                    ================================================= */

                    if (
                        data.type ===
                        "join"
                    ) {

                        const username =
                            cleanText(
                                data.username,
                                30
                            );

                        const roomName =
                            cleanText(
                                data.room,
                                100
                            );

                        const password =
                            typeof data.password ===
                            "string"
                                ? data.password.slice(
                                    0,
                                    100
                                )
                                : "";

                        if (
                            !username ||
                            !roomName
                        ) {

                            send(
                                ws,
                                {
                                    type:
                                        "error",

                                    message:
                                        "Name and room are required."
                                }
                            );

                            return;
                        }

                        await joinRoom(
                            ws,
                            roomName,
                            username,
                            password
                        );

                        return;
                    }

                    /* =================================================
                       REQUIRE ROOM
                    ================================================= */

                    if (
                        !ws.room ||
                        !ws.username
                    ) {

                        send(
                            ws,
                            {
                                type:
                                    "error",

                                message:
                                    "You are not in a room."
                            }
                        );

                        return;
                    }

                    const room =
                        rooms.get(
                            ws.room
                        );

                    if (!room) {
                        return;
                    }

                    /* =================================================
                       MESSAGE
                    ================================================= */

                    if (
                        data.type ===
                        "message"
                    ) {

                        const message =
                            cleanText(
                                data.message,
                                1000
                            );

                        if (!message) {
                            return;
                        }

                        let replyTo =
                            null;

                        if (
                            data.replyTo !==
                                null &&
                            data.replyTo !==
                                undefined
                        ) {

                            const replyId =
                                Number(
                                    data.replyTo
                                );

                            if (
                                Number.isSafeInteger(
                                    replyId
                                )
                            ) {

                                replyTo =
                                    replyId;
                            }
                        }

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
                                VALUES
                                    (
                                        $1,
                                        $2,
                                        $3,
                                        $4
                                    )
                                RETURNING
                                    id,
                                    time
                                `,
                                [
                                    ws.room,
                                    ws.username,
                                    message,
                                    replyTo
                                ]
                            );

                        const saved =
                            result.rows[0];

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "message",

                                id:
                                    saved.id,

                                username:
                                    ws.username,

                                message:
                                    message,

                                time:
                                    saved.time,

                                edited:
                                    false,

                                replyTo:
                                    replyTo,

                                reactions:
                                    {}
                            }
                        );

                        return;
                    }

                    /* =================================================
                       EDIT MESSAGE
                    ================================================= */

                    if (
                        data.type ===
                        "edit"
                    ) {

                        const id =
                            Number(
                                data.id
                            );

                        const message =
                            cleanText(
                                data.message,
                                1000
                            );

                        if (
                            !Number.isSafeInteger(
                                id
                            ) ||
                            !message
                        ) {

                            return;
                        }

                        const result =
                            await pool.query(
                                `
                                UPDATE messages
                                SET
                                    message = $1,
                                    edited = TRUE

                                WHERE
                                    id = $2
                                    AND room = $3
                                    AND username = $4
                                    AND deleted = FALSE

                                RETURNING id
                                `,
                                [
                                    message,
                                    id,
                                    ws.room,
                                    ws.username
                                ]
                            );

                        if (
                            result.rows.length ===
                            0
                        ) {

                            send(
                                ws,
                                {
                                    type:
                                        "error",

                                    message:
                                        "You can only edit your own messages."
                                }
                            );

                            return;
                        }

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "messageEdited",

                                id:
                                    id,

                                message:
                                    message
                            }
                        );

                        return;
                    }

                    /* =================================================
                       DELETE MESSAGE
                    ================================================= */

                    if (
                        data.type ===
                        "delete"
                    ) {

                        const id =
                            Number(
                                data.id
                            );

                        if (
                            !Number.isSafeInteger(
                                id
                            )
                        ) {

                            return;
                        }

                        const result =
                            await pool.query(
                                `
                                UPDATE messages
                                SET
                                    deleted = TRUE,
                                    message = '[message deleted]'

                                WHERE
                                    id = $1
                                    AND room = $2
                                    AND username = $3
                                    AND deleted = FALSE

                                RETURNING id
                                `,
                                [
                                    id,
                                    ws.room,
                                    ws.username
                                ]
                            );

                        if (
                            result.rows.length ===
                            0
                        ) {

                            send(
                                ws,
                                {
                                    type:
                                        "error",

                                    message:
                                        "You can only delete your own messages."
                                }
                            );

                            return;
                        }

                        await pool.query(
                            `
                            DELETE FROM reactions
                            WHERE message_id = $1
                            `,
                            [
                                id
                            ]
                        );

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "messageDeleted",

                                id:
                                    id
                            }
                        );

                        return;
                    }

                    /* =================================================
                       REACTION
                    ================================================= */

                    if (
                        data.type ===
                        "reaction"
                    ) {

                        const id =
                            Number(
                                data.id
                            );

                        const reaction =
                            cleanText(
                                data.reaction,
                                10
                            );

                        const allowed = [
                            "👍",
                            "❤️",
                            "😂",
                            "😮",
                            "😢",
                            "😡"
                        ];

                        if (
                            !Number.isSafeInteger(
                                id
                            ) ||
                            !allowed.includes(
                                reaction
                            )
                        ) {

                            return;
                        }

                        const messageResult =
                            await pool.query(
                                `
                                SELECT id
                                FROM messages

                                WHERE
                                    id = $1
                                    AND room = $2
                                    AND deleted = FALSE
                                `,
                                [
                                    id,
                                    ws.room
                                ]
                            );

                        if (
                            messageResult.rows.length ===
                            0
                        ) {

                            return;
                        }

                        const existing =
                            await pool.query(
                                `
                                SELECT 1
                                FROM reactions

                                WHERE
                                    message_id = $1
                                    AND username = $2
                                    AND reaction = $3
                                `,
                                [
                                    id,
                                    ws.username,
                                    reaction
                                ]
                            );

                        if (
                            existing.rows.length
                        ) {

                            await pool.query(
                                `
                                DELETE FROM reactions

                                WHERE
                                    message_id = $1
                                    AND username = $2
                                    AND reaction = $3
                                `,
                                [
                                    id,
                                    ws.username,
                                    reaction
                                ]
                            );

                        } else {

                            await pool.query(
                                `
                                INSERT INTO reactions
                                    (
                                        message_id,
                                        username,
                                        reaction
                                    )
                                VALUES
                                    (
                                        $1,
                                        $2,
                                        $3
                                    )
                                ON CONFLICT DO NOTHING
                                `,
                                [
                                    id,
                                    ws.username,
                                    reaction
                                ]
                            );
                        }

                        const reactions =
                            await getReactions(
                                id
                            );

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "reactionUpdate",

                                id:
                                    id,

                                reactions:
                                    reactions
                            }
                        );

                        return;
                    }

                    /* =================================================
                       TYPING
                    ================================================= */

                    if (
                        data.type ===
                        "typing"
                    ) {

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "typing",

                                username:
                                    ws.username,

                                typing:
                                    Boolean(
                                        data.typing
                                    )
                            },
                            ws
                        );

                        return;
                    }

                    /* =================================================
                       OWNER CHECK
                    ================================================= */

                    function ownerOnly() {

                        if (
                            !ws.isOwner ||
                            ws.username !==
                                room.owner
                        ) {

                            send(
                                ws,
                                {
                                    type:
                                        "error",

                                    message:
                                        "Only the room owner can do that."
                                }
                            );

                            return false;
                        }

                        return true;
                    }

                    /* =================================================
                       KICK USER
                    ================================================= */

                    if (
                        data.type ===
                        "kick"
                    ) {

                        if (
                            !ownerOnly()
                        ) {

                            return;
                        }

                        const targetName =
                            cleanText(
                                data.username,
                                30
                            );

                        if (
                            !targetName ||
                            targetName ===
                                ws.username
                        ) {

                            return;
                        }

                        let target =
                            null;

                        for (
                            const client
                            of room.users
                        ) {

                            if (
                                client.username ===
                                targetName
                            ) {

                                target =
                                    client;

                                break;
                            }
                        }

                        if (!target) {

                            send(
                                ws,
                                {
                                    type:
                                        "error",

                                    message:
                                        "That user is not online."
                                }
                            );

                            return;
                        }

                        send(
                            target,
                            {
                                type:
                                    "kicked",

                                message:
                                    "You were kicked from the room by the owner."
                            }
                        );

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "system",

                                message:
                                    `${target.username} was kicked from the room.`
                            },
                            target
                        );

                        room.users.delete(
                            target
                        );

                        try {
                            target.close();
                        } catch {}

                        updateUsers(
                            ws.room
                        );

                        return;
                    }

                    /* =================================================
                       CHANGE / REMOVE PASSWORD
                    ================================================= */

                    if (
                        data.type ===
                        "changePassword"
                    ) {

                        if (
                            !ownerOnly()
                        ) {

                            return;
                        }

                        let newPassword =
                            typeof data.password ===
                            "string"
                                ? data.password
                                : "";

                        newPassword =
                            newPassword.slice(
                                0,
                                100
                            );

                        const hashed =
                            hashPassword(
                                newPassword
                            );

                        await pool.query(
                            `
                            UPDATE rooms
                            SET password = $1
                            WHERE room = $2
                            `,
                            [
                                hashed,
                                ws.room
                            ]
                        );

                        room.hasPassword =
                            Boolean(
                                newPassword
                            );

                        sendRoomSettings(
                            ws.room
                        );

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "system",

                                message:
                                    newPassword
                                        ? "The room password was changed."
                                        : "The room password was removed."
                            }
                        );

                        send(
                            ws,
                            {
                                type:
                                    "passwordChanged",

                                message:
                                    newPassword
                                        ? "Room password changed successfully."
                                        : "Room password removed."
                            }
                        );

                        return;
                    }

                    /* =================================================
                       RENAME ROOM
                    ================================================= */

                    if (
                        data.type ===
                        "renameRoom"
                    ) {

                        if (
                            !ownerOnly()
                        ) {

                            return;
                        }

                        const newRoomName =
                            cleanText(
                                data.newName ||
                                data.name,
                                100
                            );

                        if (!newRoomName) {

                            send(
                                ws,
                                {
                                    type:
                                        "error",

                                    message:
                                        "Enter a room name."
                                }
                            );

                            return;
                        }

                        if (
                            newRoomName ===
                            ws.room
                        ) {

                            return;
                        }

                        const existing =
                            await getRoom(
                                newRoomName
                            );

                        if (existing) {

                            send(
                                ws,
                                {
                                    type:
                                        "error",

                                    message:
                                        "That room name is already being used."
                                }
                            );

                            return;
                        }

                        const oldRoomName =
                            ws.room;

                        /* Update rooms table */

                        await pool.query(
                            `
                            UPDATE rooms
                            SET room = $1
                            WHERE room = $2
                            `,
                            [
                                newRoomName,
                                oldRoomName
                            ]
                        );

                        /* Update messages */

                        await pool.query(
                            `
                            UPDATE messages
                            SET room = $1
                            WHERE room = $2
                            `,
                            [
                                newRoomName,
                                oldRoomName
                            ]
                        );

                        /* Move memory room */

                        rooms.delete(
                            oldRoomName
                        );

                        rooms.set(
                            newRoomName,
                            room
                        );

                        room.name =
                            newRoomName;

                        /* Update every client */

                        for (
                            const client
                            of room.users
                        ) {

                            client.room =
                                newRoomName;

                            send(
                                client,
                                {
                                    type:
                                        "roomRenamed",

                                    oldName:
                                        oldRoomName,

                                    newName:
                                        newRoomName
                                }
                            );
                        }

                        broadcast(
                            newRoomName,
                            {
                                type:
                                    "system",

                                message:
                                    `The room was renamed to "${newRoomName}".`
                            }
                        );

                        sendRoomSettings(
                            newRoomName
                        );

                        return;
                    }

                    /* =================================================
                       TRANSFER OWNERSHIP
                    ================================================= */

                    if (
                        data.type ===
                            "transferOwner" ||
                        data.type ===
                            "transferOwnership"
                    ) {

                        if (
                            !ownerOnly()
                        ) {

                            return;
                        }

                        const targetName =
                            cleanText(
                                data.username,
                                30
                            );

                        if (!targetName) {
                            return;
                        }

                        if (
                            targetName.toLowerCase() ===
                            ws.username.toLowerCase()
                        ) {

                            return;
                        }

                        let target =
                            null;

                        for (
                            const client
                            of room.users
                        ) {

                            if (
                                client.username ===
                                targetName
                            ) {

                                target =
                                    client;

                                break;
                            }
                        }

                        if (!target) {

                            send(
                                ws,
                                {
                                    type:
                                        "error",

                                    message:
                                        "The new owner must be online."
                                }
                            );

                            return;
                        }

                        const oldOwner =
                            room.owner;

                        room.owner =
                            target.username;

                        await pool.query(
                            `
                            UPDATE rooms
                            SET owner = $1
                            WHERE room = $2
                            `,
                            [
                                room.owner,
                                ws.room
                            ]
                        );

                        /* Update owner status */

                        for (
                            const client
                            of room.users
                        ) {

                            client.isOwner =
                                client.username
                                    .toLowerCase() ===
                                room.owner
                                    .toLowerCase();

                            send(
                                client,
                                {
                                    type:
                                        "owner",

                                    owner:
                                        room.owner,

                                    isOwner:
                                        client.isOwner
                                }
                            );
                        }

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "system",

                                message:
                                    `${oldOwner} transferred ownership to ${room.owner}.`
                            }
                        );

                        sendRoomSettings(
                            ws.room
                        );

                        return;
                    }

                    /* =================================================
                       LOCK / UNLOCK
                    ================================================= */

                    if (
                        data.type ===
                            "toggleLock" ||
                        data.type ===
                            "setLock"
                    ) {

                        if (
                            !ownerOnly()
                        ) {

                            return;
                        }

                        let locked;

                        if (
                            data.type ===
                            "toggleLock"
                        ) {

                            locked =
                                !room.locked;

                        } else {

                            locked =
                                Boolean(
                                    data.locked
                                );
                        }

                        await pool.query(
                            `
                            UPDATE rooms
                            SET locked = $1
                            WHERE room = $2
                            `,
                            [
                                locked,
                                ws.room
                            ]
                        );

                        room.locked =
                            locked;

                        sendRoomSettings(
                            ws.room
                        );

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "roomLockChanged",

                                locked:
                                    locked,

                                message:
                                    locked
                                        ? "The room has been locked. New users cannot join."
                                        : "The room has been unlocked."
                            }
                        );

                        return;
                    }

                    /* =================================================
                       DELETE ROOM
                    ================================================= */

                    if (
                        data.type ===
                        "deleteRoom"
                    ) {

                        if (
                            !ownerOnly()
                        ) {

                            return;
                        }

                        const roomName =
                            ws.room;

                        /* Delete reactions */

                        await pool.query(
                            `
                            DELETE FROM reactions

                            WHERE message_id IN (
                                SELECT id
                                FROM messages
                                WHERE room = $1
                            )
                            `,
                            [
                                roomName
                            ]
                        );

                        /* Delete messages */

                        await pool.query(
                            `
                            DELETE FROM messages
                            WHERE room = $1
                            `,
                            [
                                roomName
                            ]
                        );

                        /* Delete room */

                        await pool.query(
                            `
                            DELETE FROM rooms
                            WHERE room = $1
                            `,
                            [
                                roomName
                            ]
                        );

                        const currentRoom =
                            rooms.get(
                                roomName
                            );

                        if (
                            currentRoom
                        ) {

                            for (
                                const client
                                of currentRoom.users
                            ) {

                                send(
                                    client,
                                    {
                                        type:
                                            "roomDeleted",

                                        message:
                                            "This room was deleted by the owner."
                                    }
                                );

                                client.room =
                                    null;

                                client.isOwner =
                                    false;

                                try {
                                    client.close();
                                } catch {}
                            }
                        }

                        rooms.delete(
                            roomName
                        );

                        return;
                    }

                    /* =================================================
                       UNKNOWN REQUEST
                    ================================================= */

                    send(
                        ws,
                        {
                            type:
                                "error",

                            message:
                                "Unknown request."
                        }
                    );

                } catch (error) {

                    console.error(
                        "WebSocket error:",
                        error
                    );

                    send(
                        ws,
                        {
                            type:
                                "error",

                            message:
                                "Server error."
                        }
                    );
                }
            }
        );

        /* =====================================================
           DISCONNECT
        ===================================================== */

        ws.on(
            "close",
            () => {

                if (!ws.room) {
                    return;
                }

                const room =
                    rooms.get(
                        ws.room
                    );

                if (!room) {
                    return;
                }

                room.users.delete(
                    ws
                );

                broadcast(
                    ws.room,
                    {
                        type:
                            "typing",

                        username:
                            ws.username,

                        typing:
                            false
                    }
                );

                if (
                    ws.username
                ) {

                    broadcast(
                        ws.room,
                        {
                            type:
                                "system",

                            message:
                                `${ws.username} left the room.`
                        }
                    );
                }

                updateUsers(
                    ws.room
                );

                if (
                    room.users.size ===
                    0
                ) {

                    rooms.delete(
                        ws.room
                    );
                }
            }
        );

        /* =====================================================
           WEBSOCKET ERROR
        ===================================================== */

        ws.on(
            "error",
            error => {

                console.error(
                    "WebSocket connection error:",
                    error.message
                );
            }
        );
    }
);

/* =========================================================
   START SERVER
========================================================= */

async function start() {

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
            "Failed to start server:",
            error
        );

        process.exit(1);
    }
}

start();
