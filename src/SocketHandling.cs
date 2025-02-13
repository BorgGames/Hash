namespace Hash;

using System.Net.Sockets;

static class SocketHandling {
    public static bool IsProbablyJustDisconnected(SocketError error)
        => error is SocketError.ConnectionReset
            or SocketError.TimedOut;
}