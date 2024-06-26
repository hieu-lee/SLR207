package rs;

import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.Socket;

public class SocketUtils {
    public static final int PORT = 12345;
    public static void write(Socket aSocket, String aMessage) {
        try {
            OutputStream myOutputStream = aSocket.getOutputStream();
            myOutputStream.write((aMessage + "\n").getBytes());
            myOutputStream.flush();
        } catch (Exception aE) {
            aE.printStackTrace();
        }
    }

    @NotNull
    public static String read(Socket aSocket) {
        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(aSocket.getInputStream()));
            StringBuilder result = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                result.append(line + "\n");
                if (line.isEmpty()) { // Stop reading if a newline character is encountered
                    break;
                }
            }
            return result.toString().trim();
        } catch (Exception aE) {
            aE.printStackTrace();
            return "";
        }
    }

}
