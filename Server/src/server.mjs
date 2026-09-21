import dotenv from "dotenv";
import http from "http";
import jwt from "jsonwebtoken";
import { Server } from "socket.io";
import { PrismaClient } from "@prisma/client";
import { PrismaPg } from "@prisma/adapter-pg";
import app from "./app.mjs";

dotenv.config();

const PORT = process.env.PORT || 3000;
// ============================================================
// PRISMA
// ============================================================

const adapter = new PrismaPg({
    connectionString: process.env.DATABASE_URL
});

const prisma = new PrismaClient({
    adapter
});


// ============================================================
// HTTP SERVER
// ============================================================

const httpServer = http.createServer(app);


// ============================================================
// SOCKET.IO
// ============================================================

const io = new Server(httpServer, {
    cors: {
        origin: [
            "https://language-bridge-orpin.vercel.app",
            "http://127.0.0.1:5500",
            "http://localhost:5500"
        ],
        methods: ["GET", "POST"],
        credentials: true
    },

    path: "/api/socket-io/socket.io"
});


// Make Socket.IO available to Express controllers.
app.set("io", io);


// ============================================================
// ONLINE USERS
// ============================================================
//
// userId -> Set of socket IDs
//
// A user can have multiple:
// - browser tabs
// - browsers
// - devices
//
// A user is offline only when their LAST socket disconnects.
//

const onlineUsers = new Map();


// ============================================================
// PRESENCE WATCHERS
// ============================================================
//
// watchedUserId -> Set of watcher socket IDs
//
// Example:
//
// User B is being viewed by:
// - User A
// - User C
//
// presenceWatchers:
//
// B -> { socketA, socketC }
//
// When B goes online/offline, only those sockets
// receive the presence:update event.
//

const presenceWatchers = new Map();


// ============================================================
// ONLINE USER HELPERS
// ============================================================

function addUserSocket(userId, socketId) {

    if (!userId || !socketId) {
        return;
    }

    const id = String(userId);

    if (!onlineUsers.has(id)) {
        onlineUsers.set(id, new Set());
    }

    onlineUsers.get(id).add(String(socketId));
}


function removeUserSocket(userId, socketId) {

    if (!userId || !socketId) {
        return false;
    }

    const id = String(userId);

    const sockets = onlineUsers.get(id);

    if (!sockets) {
        return false;
    }

    sockets.delete(String(socketId));

    // User still has another active socket.
    if (sockets.size > 0) {
        return false;
    }

    // No sockets remain.
    onlineUsers.delete(id);

    return true;
}


function isUserOnline(userId) {

    if (!userId) {
        return false;
    }

    return onlineUsers.has(String(userId));
}


// ============================================================
// PRESENCE WATCHER HELPERS
// ============================================================

function addWatcher(watcherSocketId, watchedUserId) {

    if (!watcherSocketId || !watchedUserId) {
        return;
    }

    const watchedId = String(watchedUserId);
    const watcherId = String(watcherSocketId);

    if (!presenceWatchers.has(watchedId)) {
        presenceWatchers.set(
            watchedId,
            new Set()
        );
    }

    presenceWatchers
        .get(watchedId)
        .add(watcherId);
}


function removeWatcher(
    watcherSocketId,
    watchedUserId
) {

    if (!watcherSocketId || !watchedUserId) {
        return;
    }

    const watchedId = String(watchedUserId);

    const watchers =
        presenceWatchers.get(watchedId);

    if (!watchers) {
        return;
    }

    watchers.delete(
        String(watcherSocketId)
    );

    if (watchers.size === 0) {
        presenceWatchers.delete(watchedId);
    }
}


function removeAllWatchers(socketId) {

    if (!socketId) {
        return;
    }

    const id = String(socketId);

    for (const watchers of presenceWatchers.values()) {
        watchers.delete(id);
    }

    for (const [watchedUserId, watchers] of presenceWatchers) {

        if (watchers.size === 0) {
            presenceWatchers.delete(
                watchedUserId
            );
        }
    }
}


function notifyWatchers(
    watchedUserId,
    online
) {

    if (!watchedUserId) {
        return;
    }

    const watchedId = String(watchedUserId);

    const watchers =
        presenceWatchers.get(watchedId);

    if (!watchers) {
        return;
    }

    for (const socketId of watchers) {

        io.to(socketId).emit(
            "presence:update",
            {
                userId: watchedId,
                online: Boolean(online)
            }
        );
    }
}


// ============================================================
// SOCKET.IO JWT AUTHENTICATION
// ============================================================
//
// Frontend connects using:
//
// io(BASE_URL, {
//     auth: {
//         token: accessToken
//     }
// });
//
// This uses the SAME JWT secret as jwtValidate.mjs.
//

io.use((socket, next) => {

    try {

        const token =
            socket.handshake.auth?.token;


        if (!token) {

            console.log(
                "Socket connection rejected: no token"
            );

            return next(
                new Error(
                    "Authentication required"
                )
            );
        }


        const decoded = jwt.verify(
            token,
            process.env.JWT_ACCESS_TOKEN_SECRET
        );


        if (!decoded?.id) {

            console.log(
                "Socket connection rejected: " +
                "token has no user ID"
            );

            return next(
                new Error(
                    "Invalid authentication token"
                )
            );
        }


        // Store the complete decoded JWT.
        socket.user = decoded;

        // Convenient user ID shortcut.
        socket.userId = String(
            decoded.id
        );


        next();

    } catch (error) {

        console.error(
            "Socket authentication failed:",
            error.message
        );

        next(
            new Error(
                "Invalid or expired access token"
            )
        );
    }

});


// ============================================================
// SOCKET CONNECTION
// ============================================================

io.on("connection", (socket) => {

    const userId = socket.userId;


    console.log(
        `User connected: ${userId} | socket: ${socket.id}`
    );


    // ========================================================
    // REGISTER ONLINE USER
    // ========================================================

    const wasOffline =
        !isUserOnline(userId);

    addUserSocket(
        userId,
        socket.id
    );


    if (wasOffline) {

        console.log(
            `User ${userId} is now online`
        );


        // Notify anyone currently watching
        // this user's profile/friend presence.
        notifyWatchers(
            userId,
            true
        );
    }


    // ========================================================
    // JOIN CHAT
    // ========================================================

    socket.on(
        "join:chat",
        async ({ chatId } = {}) => {

            if (!chatId) {
                return;
            }


            try {

                const chat =
                    await prisma.chat.findUnique({

                        where: {
                            id: String(chatId)
                        },

                        select: {
                            id: true,
                            user1Id: true,
                            user2Id: true
                        }

                    });


                // Chat doesn't exist.
                if (!chat) {

                    console.log(
                        `Chat not found: ${chatId}`
                    );

                    return;
                }


                // =================================================
                // SECURITY
                // =================================================
                //
                // Only participants can enter the room.
                //

                const isParticipant =
                    chat.user1Id === userId ||
                    chat.user2Id === userId;


                if (!isParticipant) {

                    console.log(
                        `Unauthorized chat access attempt. ` +
                        `User ${userId}, chat ${chatId}`
                    );

                    return;
                }


                // =================================================
                // JOIN SOCKET ROOM
                // =================================================

                socket.join(
                    String(chatId)
                );


                console.log(
                    `User ${userId} joined chat ${chatId}`
                );


                // =================================================
                // FIND OTHER PARTICIPANT
                // =================================================

                const otherUserId =
                    chat.user1Id === userId
                        ? chat.user2Id
                        : chat.user1Id;


                // =================================================
                // SEND CURRENT PRESENCE TO THIS USER
                // =================================================

                socket.emit(
                    "presence:update",
                    {
                        userId: String(
                            otherUserId
                        ),

                        online:
                            isUserOnline(
                                otherUserId
                            )
                    }
                );


                // =================================================
                // TELL OTHER USERS IN CHAT THAT
                // THIS USER IS ONLINE
                // =================================================

                socket
                    .to(String(chatId))
                    .emit(
                        "presence:update",
                        {
                            userId,

                            online: true
                        }
                    );


            } catch (error) {

                console.error(
                    "join:chat error:",
                    error
                );

            }

        }
    );


    // ========================================================
    // LEAVE CHAT
    // ========================================================

    socket.on(
        "leave:chat",
        ({ chatId } = {}) => {

            if (!chatId) {
                return;
            }


            socket.leave(
                String(chatId)
            );


            console.log(
                `User ${userId} left chat ${chatId}`
            );

        }
    );


    // ========================================================
    // GET PRESENCE
    // ========================================================
    //
    // Used for profile/friend/discover pages.
    //
    // Frontend:
    //
    // socket.emit("presence:get", {
    //     userId: friendId
    // });
    //
    // Backend immediately responds with:
    //
    // presence:update
    //
    // and keeps the socket registered as a watcher.
    //

    socket.on(
        "presence:get",
        ({ userId: watchedUserId } = {}) => {

            if (!watchedUserId) {
                return;
            }


            const watchedId =
                String(watchedUserId);


            // =================================================
            // SWITCH WATCHED USER
            // =================================================

            if (
                socket.watching &&
                String(socket.watching) !== watchedId
            ) {

                removeWatcher(
                    socket.id,
                    socket.watching
                );
            }


            // Remember the user being watched.
            socket.watching = watchedId;


            // Register this socket as a watcher.
            addWatcher(
                socket.id,
                watchedId
            );


            // =================================================
            // SEND CURRENT STATUS IMMEDIATELY
            // =================================================

            socket.emit(
                "presence:update",
                {
                    userId: watchedId,

                    online:
                        isUserOnline(
                            watchedId
                        )
                }
            );

        }
    );


    // ========================================================
    // TYPING START
    // ========================================================

    socket.on(
        "typing:start",
        async ({ chatId } = {}) => {

            if (!chatId) {
                return;
            }


            try {

                const chat =
                    await prisma.chat.findUnique({

                        where: {
                            id: String(chatId)
                        },

                        select: {
                            user1Id: true,
                            user2Id: true
                        }

                    });


                if (!chat) {
                    return;
                }


                // =================================================
                // SECURITY
                // =================================================

                const isParticipant =
                    chat.user1Id === userId ||
                    chat.user2Id === userId;


                if (!isParticipant) {
                    return;
                }


                // =================================================
                // SEND ONLY TO OTHER PARTICIPANT
                // =================================================

                socket
                    .to(String(chatId))
                    .emit(
                        "typing:start",
                        {
                            chatId:
                                String(chatId),

                            userId
                        }
                    );


            } catch (error) {

                console.error(
                    "typing:start error:",
                    error
                );

            }

        }
    );


    // ========================================================
    // TYPING STOP
    // ========================================================

    socket.on(
        "typing:stop",
        async ({ chatId } = {}) => {

            if (!chatId) {
                return;
            }


            try {

                const chat =
                    await prisma.chat.findUnique({

                        where: {
                            id: String(chatId)
                        },

                        select: {
                            user1Id: true,
                            user2Id: true
                        }

                    });


                if (!chat) {
                    return;
                }


                // =================================================
                // SECURITY
                // =================================================

                const isParticipant =
                    chat.user1Id === userId ||
                    chat.user2Id === userId;


                if (!isParticipant) {
                    return;
                }


                // =================================================
                // TELL OTHER PARTICIPANT
                // =================================================

                socket
                    .to(String(chatId))
                    .emit(
                        "typing:stop",
                        {
                            chatId:
                                String(chatId),

                            userId
                        }
                    );


            } catch (error) {

                console.error(
                    "typing:stop error:",
                    error
                );

            }

        }
    );


    // ========================================================
    // DISCONNECT
    // ========================================================

    socket.on(
        "disconnect",
        () => {

            console.log(
                `Socket disconnected: ` +
                `${socket.id} | user: ${userId}`
            );


            // =================================================
            // REMOVE PROFILE PRESENCE WATCHER
            // =================================================

            removeAllWatchers(
                socket.id
            );


            // =================================================
            // REMOVE SOCKET FROM ONLINE USER
            // =================================================

            const becameOffline =
                removeUserSocket(
                    userId,
                    socket.id
                );


            // User still has another tab/device.
            if (!becameOffline) {
                return;
            }


            console.log(
                `User ${userId} is now offline`
            );


            // =================================================
            // NOTIFY PROFILE/FRIEND WATCHERS
            // =================================================

            notifyWatchers(
                userId,
                false
            );


            // =================================================
            // NOTIFY USERS CURRENTLY IN RELEVANT CHATS
            // =================================================

            prisma.chat.findMany({

                where: {
                    OR: [
                        {
                            user1Id: userId
                        },
                        {
                            user2Id: userId
                        }
                    ]
                },

                select: {
                    id: true
                }

            })
            .then((chats) => {

                for (const chat of chats) {

                    io.to(
                        String(chat.id)
                    ).emit(
                        "presence:update",
                        {
                            userId,
                            online: false
                        }
                    );

                }

            })
            .catch((error) => {

                console.error(
                    "Error updating offline presence:",
                    error
                );

            });

        }
    );

});


// ============================================================
// START SERVER
// ============================================================

httpServer.listen(
    PORT,
    "0.0.0.0",
    () => {

        console.log(
            `I am listening on PORT ${PORT}`
        );

    }
);


// ============================================================
// EXPORT SOCKET.IO
// ============================================================

export { io };