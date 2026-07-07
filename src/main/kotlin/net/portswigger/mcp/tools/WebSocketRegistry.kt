package net.portswigger.mcp.tools

import burp.api.montoya.websocket.Direction
import burp.api.montoya.websocket.extension.ExtensionWebSocket
import burp.api.montoya.websocket.extension.ExtensionWebSocketMessageHandler
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * Tracks WebSocket connections opened via the MCP `create_websocket` tool.
 *
 * MCP tool calls are stateless, so a connection created in one call must be
 * retrievable by later `send_websocket_message` / `get_websocket_messages` /
 * `close_websocket` calls. Each managed connection is assigned a stable id and
 * buffers messages received from the server until the client reads them.
 */
object WebSocketRegistry {

    data class BufferedMessage(val direction: String, val type: String, val payload: String)

    private class ManagedWebSocket(val webSocket: ExtensionWebSocket) {
        val received = ConcurrentLinkedQueue<BufferedMessage>()

        @Volatile
        var closed = false
    }

    private val connections = ConcurrentHashMap<String, ManagedWebSocket>()
    private val counter = AtomicInteger(0)

    /** Registers a newly created WebSocket, wiring up message buffering, and returns its id. */
    fun register(webSocket: ExtensionWebSocket): String {
        val id = "ws-${counter.incrementAndGet()}"
        val managed = ManagedWebSocket(webSocket)
        connections[id] = managed

        webSocket.registerMessageHandler(object : ExtensionWebSocketMessageHandler {
            override fun textMessageReceived(message: burp.api.montoya.websocket.TextMessage) {
                managed.received.add(BufferedMessage(message.direction().name, "text", message.payload()))
            }

            override fun binaryMessageReceived(message: burp.api.montoya.websocket.BinaryMessage) {
                managed.received.add(
                    BufferedMessage(message.direction().name, "binary", message.payload().toString())
                )
            }

            override fun onClose() {
                managed.closed = true
            }
        })

        return id
    }

    fun sendText(id: String, message: String): Boolean {
        val managed = connections[id] ?: return false
        managed.webSocket.sendTextMessage(message)
        managed.received.add(BufferedMessage(Direction.CLIENT_TO_SERVER.name, "text", message))
        return true
    }

    /** Drains and returns all buffered messages for the connection, or null if the id is unknown. */
    fun drainMessages(id: String): List<BufferedMessage>? {
        val managed = connections[id] ?: return null
        val drained = mutableListOf<BufferedMessage>()
        while (true) {
            val next = managed.received.poll() ?: break
            drained.add(next)
        }
        return drained
    }

    fun isClosed(id: String): Boolean = connections[id]?.closed ?: true

    fun close(id: String): Boolean {
        val managed = connections.remove(id) ?: return false
        managed.webSocket.close()
        return true
    }

    fun openIds(): List<String> = connections.keys.sorted()
}
