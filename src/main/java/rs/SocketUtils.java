package rs;

import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

public class SocketUtils {
    public static final int PORT = 12345;
    public static void write(Socket aSocket, String aMessage) {
        try {
            OutputStream myOutputStream = aSocket.getOutputStream();
            myOutputStream.write((aMessage).getBytes());
            myOutputStream.flush();
        } catch (Exception aE) {
            aE.printStackTrace();
        }
    }

    @NotNull
    public static String read(Socket aSocket) {
        try {
            InputStream myInputStream = aSocket.getInputStream();
            ByteArrayOutputStream myBuffer = new ByteArrayOutputStream();
            byte[] myData = new byte[1024];
            int myLength;
            while ((myLength = myInputStream.read(myData)) != -1) {
                myBuffer.write(myData, 0, myLength);
            }
            return myBuffer.toString("UTF-8");
        } catch (Exception aE) {
            aE.printStackTrace();
            return "";
        }
    }

}
