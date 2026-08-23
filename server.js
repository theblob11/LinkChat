const express = require("express");
const http = require("http");
const WebSocket = require("ws");
const path = require("path");

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

const PORT = process.env.PORT || 3000;

// Store connected users by room
const rooms = new Map();

app.use(express.static(path.join(__dirname, "public")));

app.use((req, res) => {
    res.sendFile(path.join(__dirname, "public", "index.html"));
});
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

    ws.on("message", (raw) => {
        let data;

        try {
            data = JSON.parse(raw.toString());
        } catch {
            return;
        }

        // Join a room
        if (data.type === "join") {
            const room = String(data.room || "").trim().slice(0, 100);
            const username = String(data.username || "Anonymous")
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

            const user = {
                ws,
                username
            };

            rooms.get(room).add(user);

            send(ws, {
                type: "joined",
                room,
                username
            });

            broadcastExcept(
                room,
                {
                    type: "system",
                    message: `${username} joined the chat.`
                },
                ws
            );

            broadcast(room, {
                type: "users",
                count: rooms.get(room).size
            });

            return;
        }

        // Send a chat message
        if (data.type === "message") {
            if (!currentRoom || !currentUser) return;

            const message = String(data.message || "")
                .trim()
                .slice(0, 1000);

            if (!message) return;

            broadcast(currentRoom, {
                type: "message",
                username: currentUser,
                message,
                time: new Date().toISOString()
            });

            return;
        }
    });

    ws.on("close", () => {
        if (!currentRoom) return;

        const users = rooms.get(currentRoom);

        if (!users) return;

        for (const user of users) {
            if (user.ws === ws) {
                users.delete(user);
                break;
            }
        }

        broadcast(currentRoom, {
            type: "system",
            message: `${currentUser || "Someone"} left the chat.`
        });

        if (users.size === 0) {
            rooms.delete(currentRoom);
        } else {
            broadcast(currentRoom, {
                type: "users",
                count: users.size
            });
        }
    });
});

server.listen(PORT, "0.0.0.0", () => {
    console.log(`Chat server running on port ${PORT}`);
});