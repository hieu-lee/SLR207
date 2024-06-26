package rs;

import org.jetbrains.annotations.NotNull;

import java.io.OutputStream;
import java.net.Socket;

public class SocketUtils {
    public static final int PORT = 12345;
    public static void write(Socket aSocket, String aMessage) {
        try {
            OutputStream myOutputStream = aSocket.getOutputStream();
            myOutputStream.write(aMessage.getBytes());
            myOutputStream.flush();
        } catch (Exception aE) {
            aE.printStackTrace();
        }
    }

    @NotNull
    public static String read(Socket aSocket) {
        try {
            byte[] myBuffer = new byte[1024];
            int myLength = aSocket.getInputStream().read(myBuffer);
            return new String(myBuffer, 0, myLength);
        } catch (Exception aE) {
            aE.printStackTrace();
            return "";
        }
    }
}
