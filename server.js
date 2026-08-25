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
    console.error("DATABASE_URL is not set.");
    process.exit(1);
}

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
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

/* =========================================================
   WEBSOCKET
========================================================= */

const wss = new WebSocket.Server({
    server
});

const rooms = new Map();

/* =========================================================
   DATABASE SETUP / MIGRATION
========================================================= */

async function setupDatabase() {

    console.log("Checking database...");

    /* =====================================================
       ROOMS
    ===================================================== */

    await pool.query(`
        CREATE TABLE IF NOT EXISTS rooms (
            id SERIAL PRIMARY KEY,
            room TEXT UNIQUE NOT NULL,
            password TEXT DEFAULT '',
            owner TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    await pool.query(`
        ALTER TABLE rooms
        ADD COLUMN IF NOT EXISTS room TEXT
    `);

    await pool.query(`
        ALTER TABLE rooms
        ADD COLUMN IF NOT EXISTS password TEXT DEFAULT ''
    `);

    await pool.query(`
        ALTER TABLE rooms
        ADD COLUMN IF NOT EXISTS owner TEXT DEFAULT ''
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
        ADD COLUMN IF NOT EXISTS room TEXT
    `);

    await pool.query(`
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS username TEXT
    `);

    await pool.query(`
        ALTER TABLE messages
        ADD COLUMN IF NOT EXISTS message TEXT
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
       PROFILES
    ===================================================== */

    await pool.query(`
        CREATE TABLE IF NOT EXISTS profiles (
            username TEXT PRIMARY KEY,
            about TEXT DEFAULT '',
            avatar_color TEXT DEFAULT '#3b82f6',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);

    await pool.query(`
        ALTER TABLE profiles
        ADD COLUMN IF NOT EXISTS about TEXT DEFAULT ''
    `);

    await pool.query(`
        ALTER TABLE profiles
        ADD COLUMN IF NOT EXISTS avatar_color TEXT DEFAULT '#3b82f6'
    `);

    await pool.query(`
        ALTER TABLE profiles
        ADD COLUMN IF NOT EXISTS created_at
        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    `);

    await pool.query(`
        ALTER TABLE profiles
        ADD COLUMN IF NOT EXISTS updated_at
        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    `);

    /* =====================================================
       FIX EXISTING OWNER
    ===================================================== */

    await pool.query(`
        UPDATE rooms
        SET owner = 'Justin'
        WHERE room = 'JJCE'
    `);

    console.log("Database ready.");
}

/* =========================================================
   HELPERS
========================================================= */

function cleanText(value, maxLength) {

    if (typeof value !== "string") {
        return "";
    }

    return value
        .trim()
        .slice(0, maxLength);
}

function send(ws, data) {

    if (
        ws &&
        ws.readyState === WebSocket.OPEN
    ) {
        ws.send(
            JSON.stringify(data)
        );
    }
}

function broadcast(
    roomName,
    data,
    except = null
) {

    const room =
        rooms.get(roomName);

    if (!room) {
        return;
    }

    for (const client of room.users) {

        if (client === except) {
            continue;
        }

        send(client, data);
    }
}

function getRoomUsers(roomName) {

    const room =
        rooms.get(roomName);

    if (!room) {
        return [];
    }

    return Array.from(room.users)
        .filter(
            client => client.username
        )
        .map(
            client => client.username
        );
}

function getRoomUserProfiles(roomName) {

    const room =
        rooms.get(roomName);

    if (!room) {
        return [];
    }

    return Array.from(room.users)
        .filter(
            client => client.username
        )
        .map(
            client => ({
                username:
                    client.username,

                about:
                    client.profile
                        ? client.profile.about
                        : "",

                avatarColor:
                    client.profile
                        ? client.profile.avatarColor
                        : "#3b82f6"
            })
        );
}

function updateUsers(roomName) {

    broadcast(
        roomName,
        {
            type: "users",

            users:
                getRoomUsers(roomName),

            profiles:
                getRoomUserProfiles(roomName)
        }
    );
}

function hashPassword(password) {

    if (!password) {
        return "";
    }

    return crypto
        .createHash("sha256")
        .update(password)
        .digest("hex");
}

/* =========================================================
   PROFILE HELPERS
========================================================= */

const ALLOWED_AVATAR_COLORS = [
    "#3b82f6",
    "#8b5cf6",
    "#ec4899",
    "#ef4444",
    "#f97316",
    "#eab308",
    "#22c55e",
    "#14b8a6",
    "#06b6d4"
];

async function getProfile(username) {

    const result =
        await pool.query(
            `
            SELECT
                username,
                about,
                avatar_color
            FROM profiles
            WHERE username = $1
            `,
            [username]
        );

    if (
        result.rows.length === 0
    ) {

        return {
            username: username,
            about: "",
            avatarColor: "#3b82f6"
        };
    }

    return {
        username:
            result.rows[0].username,

        about:
            result.rows[0].about || "",

        avatarColor:
            result.rows[0].avatar_color ||
            "#3b82f6"
    };
}

async function createProfileIfNeeded(
    username
) {

    await pool.query(
        `
        INSERT INTO profiles
            (
                username,
                about,
                avatar_color
            )

        VALUES
            ($1, '', '#3b82f6')

        ON CONFLICT (username)
        DO NOTHING
        `,
        [username]
    );

    return getProfile(username);
}

async function saveProfile(
    username,
    about,
    avatarColor
) {

    if (
        !ALLOWED_AVATAR_COLORS.includes(
            avatarColor
        )
    ) {

        avatarColor =
            "#3b82f6";
    }

    const result =
        await pool.query(
            `
            INSERT INTO profiles
                (
                    username,
                    about,
                    avatar_color,
                    updated_at
                )

            VALUES
                ($1, $2, $3, CURRENT_TIMESTAMP)

            ON CONFLICT (username)

            DO UPDATE SET
                about = EXCLUDED.about,
                avatar_color =
                    EXCLUDED.avatar_color,
                updated_at =
                    CURRENT_TIMESTAMP

            RETURNING
                username,
                about,
                avatar_color
            `,
            [
                username,
                about,
                avatarColor
            ]
        );

    return {
        username:
            result.rows[0].username,

        about:
            result.rows[0].about || "",

        avatarColor:
            result.rows[0].avatar_color ||
            "#3b82f6"
    };
}

/* =========================================================
   ROOM DATABASE
========================================================= */

async function getRoom(roomName) {

    const result =
        await pool.query(
            `
            SELECT
                room,
                password,
                owner
            FROM rooms
            WHERE room = $1
            `,
            [roomName]
        );

    if (
        result.rows.length === 0
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
                    owner
                )

            VALUES
                ($1, $2, $3)

            RETURNING
                room,
                password,
                owner
            `,
            [
                roomName,
                hashPassword(password),
                owner
            ]
        );

    return result.rows[0];
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
                                COUNT(*)::int AS count

                            FROM reactions r

                            WHERE
                                r.message_id = m.id

                            GROUP BY
                                r.reaction
                        ) reaction_counts
                    ),
                    '{}'::json
                ) AS reactions

            FROM messages m

            WHERE
                m.room = $1

            ORDER BY
                m.id ASC

            LIMIT 500
            `,
            [roomName]
        );

    for (
        const row of result.rows
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
                    row.reactions || {}
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
            [messageId]
        );

    const reactions = {};

    for (
        const row of result.rows
    ) {

        reactions[
            row.reaction
        ] = row.count;
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
        await getRoom(roomName);

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

        /*
           Keep Justin as the owner
           of the existing JJCE room.
        */

        if (
            roomName === "JJCE"
        ) {

            await pool.query(
                `
                UPDATE rooms

                SET
                    owner = $1

                WHERE
                    room = $2
                `,
                [
                    "Justin",
                    "JJCE"
                ]
            );

            databaseRoom.owner =
                "Justin";
        }

        const suppliedPassword =
            hashPassword(password);

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
        rooms.get(roomName);

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
       PROFILE
    ===================================================== */

    const profile =
        await createProfileIfNeeded(
            username
        );

    /* =====================================================
       CREATE MEMORY ROOM
    ===================================================== */

    if (!rooms.has(roomName)) {

        rooms.set(
            roomName,
            {
                owner:
                    databaseRoom.owner,

                users:
                    new Set()
            }
        );
    }

    const room =
        rooms.get(roomName);

    room.owner =
        databaseRoom.owner;

    ws.room =
        roomName;

    ws.username =
        username;

    ws.profile =
        profile;

    ws.isOwner =
        username.toLowerCase() ===
        room.owner.toLowerCase();

    room.users.add(ws);

    /* =====================================================
       JOIN RESPONSE
    ===================================================== */

    send(
        ws,
        {
            type:
                "joined",

            room:
                roomName,

            owner:
                room.owner,

            isOwner:
                ws.isOwner,

            profile:
                profile
        }
    );

    /* =====================================================
       LOAD HISTORY
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

    updateUsers(roomName);

    return true;
}

/* =========================================================
   WEBSOCKET
========================================================= */

wss.on(
    "connection",
    ws => {

        ws.room = null;
        ws.username = null;
        ws.isOwner = false;
        ws.profile = null;

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

                    /* =========================================
                       JOIN
                    ========================================= */

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

                    /* =========================================
                       REQUIRE ROOM
                    ========================================= */

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

                    /* =========================================
                       GET PROFILE
                    ========================================= */

                    if (
                        data.type ===
                        "getProfile"
                    ) {

                        const targetUsername =
                            cleanText(
                                data.username,
                                30
                            );

                        if (!targetUsername) {
                            return;
                        }

                        /*
                           Only allow profiles of
                           people currently in this room.
                        */

                        let target = null;

                        for (
                            const client
                            of room.users
                        ) {

                            if (
                                client.username
                                    .toLowerCase() ===
                                targetUsername
                                    .toLowerCase()
                            ) {

                                target = client;

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
                                        "That user is not currently in this room."
                                }
                            );

                            return;
                        }

                        const profile =
                            target.profile ||
                            await getProfile(
                                target.username
                            );

                        send(
                            ws,
                            {
                                type:
                                    "profile",

                                profile:
                                    profile,

                                isOwner:
                                    target.username
                                        .toLowerCase() ===
                                    room.owner
                                        .toLowerCase()
                            }
                        );

                        return;
                    }

                    /* =========================================
                       UPDATE PROFILE
                    ========================================= */

                    if (
                        data.type ===
                        "updateProfile"
                    ) {

                        let about =
                            typeof data.about ===
                            "string"
                                ? data.about
                                : "";

                        about =
                            about
                                .trim()
                                .slice(
                                    0,
                                    160
                                );

                        let avatarColor =
                            typeof data.avatarColor ===
                            "string"
                                ? data.avatarColor
                                : "#3b82f6";

                        if (
                            !ALLOWED_AVATAR_COLORS
                                .includes(
                                    avatarColor
                                )
                        ) {

                            avatarColor =
                                "#3b82f6";
                        }

                        const profile =
                            await saveProfile(
                                ws.username,
                                about,
                                avatarColor
                            );

                        ws.profile =
                            profile;

                        /* -----------------------------------------
                           Tell the user their profile was updated.
                        ----------------------------------------- */

                        send(
                            ws,
                            {
                                type:
                                    "profileUpdated",

                                profile:
                                    profile
                            }
                        );

                        /* -----------------------------------------
                           Update everyone in the room.
                        ----------------------------------------- */

                        updateUsers(
                            ws.room
                        );

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "profileUpdated",

                                profile:
                                    profile
                            },
                            ws
                        );

                        return;
                    }

                    /* =========================================
                       MESSAGE
                    ========================================= */

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
                                    ($1, $2, $3, $4)

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

                    /* =========================================
                       EDIT
                    ========================================= */

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

                    /* =========================================
                       DELETE
                    ========================================= */

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
                                    message =
                                        '[message deleted]'

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

                    /* =========================================
                       REACTION
                    ========================================= */

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
                            messageResult.rows
                                .length === 0
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
                                    ($1, $2, $3)

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

                    /* =========================================
                       TYPING
                    ========================================= */

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

                    /* =========================================
                       KICK
                    ========================================= */

                    if (
                        data.type ===
                        "kick"
                    ) {

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
                                        "Only the room owner can kick users."
                                }
                            );

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

                    /* =========================================
                       CHANGE PASSWORD
                    ========================================= */

                    if (
                        data.type ===
                        "changePassword"
                    ) {

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
                                        "Only the room owner can change the password."
                                }
                            );

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

                        await pool.query(
                            `
                            UPDATE rooms

                            SET
                                password = $1

                            WHERE
                                room = $2
                            `,
                            [
                                hashPassword(
                                    newPassword
                                ),
                                ws.room
                            ]
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

                        broadcast(
                            ws.room,
                            {
                                type:
                                    "system",

                                message:
                                    "The room owner changed the room password."
                            },
                            ws
                        );

                        return;
                    }

                    /* =========================================
                       UNKNOWN REQUEST
                    ========================================= */

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

                if (ws.username) {

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
                    room.users.size === 0
                ) {

                    rooms.delete(
                        ws.room
                    );
                }
            }
        );

        /* =====================================================
           SOCKET ERROR
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
