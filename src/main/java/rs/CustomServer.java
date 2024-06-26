package rs;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class CustomServer {
    private final CustomFTPServer theCustomFTPServer;
    private ServerSocket theServerSocket;
    private static final String THE_MAP_FILE_SUFFIX = "_map.txt";
    private static final String THE_REDUCE_FILE_SUFFIX = "_reduce.txt";
    private int theNumberOfServers;
    private String theServerName;

    public static void main(String[] args) {
        CustomFTPServer myCustomFTPServer = new CustomFTPServer(CustomFTPCredential.getInstance());
        CustomServer myCustomServer = new CustomServer(myCustomFTPServer);
        myCustomServer.run();
    }

    public CustomServer(CustomFTPServer aCustomFTPServer) {
        theCustomFTPServer = aCustomFTPServer;
        try {
            theServerSocket = new ServerSocket(SocketUtils.PORT);
        } catch (Exception aE) {
            aE.printStackTrace();
        }

        System.out.println("Socket server started on port " + SocketUtils.PORT);
        theCustomFTPServer.start();
    }

    private Stream<String> getWords(String aLine) {
        return Pattern.compile("\\P{L}+")
                .splitAsStream(aLine)
                .filter(word -> !word.isEmpty());
    }


    private void getNumberOfServersAndServerName(Socket aClientSocket) {
        int myNumberOfServers = -1;
        try {
            String[] myMessage = SocketUtils.read(aClientSocket).split(" ", 2);
            myNumberOfServers = Integer.parseInt(myMessage[0]);
            theServerName = myMessage[1];
        } catch (Exception aE) {
            aE.printStackTrace();
        }
        theNumberOfServers = myNumberOfServers;
    }

    private String[] getServerNames() {
        String[] myServers = new String[theNumberOfServers];
        BufferedReader myReader;
        try {
            myReader = new BufferedReader(new FileReader(theCustomFTPServer.getHomeDirectory() + "/machines.txt"));
            for (int i = 0; i < theNumberOfServers; i++) {
                try {
                    myServers[i] = myReader.readLine().trim();
                    if (myServers[i] == null) {
                        myReader.close();
                        throw new RuntimeException("Not enough servers in machines.txt");
                    }
                } catch (Exception aE) {
                    aE.printStackTrace();
                }
            }
        } catch (FileNotFoundException aE) {
            aE.printStackTrace();
        }
        return myServers;
    }

    private StringBuilder[] mapPhase1() {
        StringBuilder[] myTokens = new StringBuilder[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            myTokens[i] = new StringBuilder();
        }
        try {
            try (Stream<String> aLines = Files.lines(Paths.get(theCustomFTPServer.getHomeDirectory(), "input.txt"))) {
                aLines.flatMap(this::getWords)
                    .forEach(aWord -> {
                        int myServerIndex = (aWord.hashCode() % theNumberOfServers + theNumberOfServers) % theNumberOfServers;
                        myTokens[myServerIndex].append(aWord).append(" ").append(1).append("\n");
                    });
            }

        } catch (Exception aE) {
            aE.printStackTrace();
        }
        return myTokens;
    }

    private void shufflePhase1(String[] aServers, StringBuilder[] aTokens, Socket aClientSocket) {
        CustomFTPClient[] myClients = new CustomFTPClient[theNumberOfServers];
        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServers[i])) {
                myClients[i] = new CustomFTPClient(theServerName + THE_MAP_FILE_SUFFIX, aTokens[i].toString(), aServers[i], CustomFTPCredential.getInstance());
                myClients[i].start();
            }
        }
        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServers[i])) {
                try {
                    myClients[i].join();
                } catch (InterruptedException aE) {
                    aE.printStackTrace();
                }
            }
        }
        SocketUtils.write(aClientSocket, "map and shuffle 1 done");
        System.out.println("map and shuffle 1 done");
    }

    private Map<String, Integer> reducePhase1(Socket aClientSocket, String[] aServers, StringBuilder[] aTokensList) {
        if (SocketUtils.read(aClientSocket).equals("reduce 1 start")) {
            System.out.println("Start reduce phase 1");
        }
        else {
            try {
                theServerSocket.close();
            } catch (IOException aE) {
                aE.printStackTrace();
            }
            throw new RuntimeException("Invalid command");
        }

        Map<String, Integer> myWordCounts = new HashMap<>();

        IntStream.range(0, theNumberOfServers)
                .mapToObj(i -> !theServerName.equals(aServers[i])
                        ? readLinesFromFile(theCustomFTPServer.getHomeDirectory(), aServers[i])
                        : Arrays.stream(aTokensList[i].toString().split("\n")))
                .flatMap(Function.identity())
                .filter(aLine -> !aLine.isEmpty())
                .map(aLine -> aLine.split(" ")).forEach(aTokens -> myWordCounts.put(aTokens[0], myWordCounts.getOrDefault(aTokens[0], 0) + Integer.parseInt(aTokens[1])));
        return myWordCounts;
    }

    private Stream<String> readLinesFromFile(String aDirectory, String aServerName) {
        try {
            return Files.lines(Paths.get(aDirectory, aServerName + THE_MAP_FILE_SUFFIX));
        } catch (IOException e) {
            e.printStackTrace();
            return Stream.empty();
        }
    }

    private List<Integer> getBoundaries(Map<String, Integer> aWordCounts, Socket aClientSocket) {
        Map<Integer, Integer> myFreqCounts = new HashMap<>();
        for (Map.Entry<String, Integer> aEntry : aWordCounts.entrySet()) {
            myFreqCounts.put(aEntry.getValue(), myFreqCounts.getOrDefault(aEntry.getValue(), 0) + 1);
        }
        List<String> myFreqCountStrings = new ArrayList<>();
        for (Map.Entry<Integer, Integer> aEntry : myFreqCounts.entrySet()) {
            myFreqCountStrings.add(aEntry.getKey() + "-" + aEntry.getValue());
        }
        SocketUtils.write(aClientSocket, String.join(" ", myFreqCountStrings));
        String myMessage = SocketUtils.read(aClientSocket);
        List<Integer> myBoundaries = new ArrayList<>();
        String[] myMessageParts = myMessage.split(" ");
        for (String myMessagePart : myMessageParts) {
            myBoundaries.add(Integer.parseInt(myMessagePart));
        }
        System.out.println("Boundaries done");
        return myBoundaries;
    }

    private int getServerIndex(int aFreq, List<Integer> aBoundaries) {
        int myServerIndex = 0;
        for (int i = 0; i < aBoundaries.size(); i++) {
            if (aFreq <= aBoundaries.get(i)) {
                myServerIndex = i;
                break;
            }
        }
        return myServerIndex;
    }

    private void shufflePhase2(Map<String, Integer> aWordCounts, String[] aServerNames, Socket aClientSocket, List<Integer> aBoundaries, StringBuilder[] aTokensList) {
        CustomFTPClient[] myClients = new CustomFTPClient[theNumberOfServers];

        for (int i = 0; i < theNumberOfServers; i++) {
            aTokensList[i] = new StringBuilder();
        }

        for (Map.Entry<String, Integer> aEntry: aWordCounts.entrySet()) {
            int myServerIndex = getServerIndex(aEntry.getValue(), aBoundaries);
            aTokensList[myServerIndex].append(aEntry.getValue()).append(" ").append(aEntry.getKey()).append("\n");
        }

        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServerNames[i])) {
                myClients[i] = new CustomFTPClient(theServerName + THE_REDUCE_FILE_SUFFIX, aTokensList[i].toString(), aServerNames[i], CustomFTPCredential.getInstance());
                myClients[i].start();
            }
        }

        for (int i = 0; i < theNumberOfServers; i++) {
            if (!theServerName.equals(aServerNames[i])) {
                try {
                    myClients[i].join();
                } catch (InterruptedException aE) {
                    aE.printStackTrace();
                }
            }
        }
        SocketUtils.write(aClientSocket, "shuffle 2 done");
        System.out.println("Shuffle 2 done");
    }

    private void reducePhase2(Socket aClientSocket, StringBuilder[] aTokensList, String[] aServerNames) {
        if (SocketUtils.read(aClientSocket).equals("reduce 2 start")) {
            System.out.println("Start reduce phase 2");
        } else {
            try {
                theServerSocket.close();
            } catch (IOException aE) {
                aE.printStackTrace();
            }
            throw new RuntimeException("Invalid command");
        }
        List<Map.Entry<Integer, String>> myWordsList = IntStream.range(0, theNumberOfServers)
                .mapToObj(i -> !theServerName.equals(aServerNames[i])
                        ? readLinesFromFile(theCustomFTPServer.getHomeDirectory(), aServerNames[i] + THE_REDUCE_FILE_SUFFIX)
                        : Arrays.stream(aTokensList[i].toString().split("\n")))
                .flatMap(Function.identity())
                .filter(aLine -> !aLine.isEmpty())
                .map(aLine -> aLine.split(" "))
                .map(aTokens -> new AbstractMap.SimpleEntry<>(Integer.parseInt(aTokens[0]), aTokens[1])).sorted((aFirst, aSecond) -> (!Objects.equals(aFirst.getKey(), aSecond.getKey())) ? aFirst.getKey() - aSecond.getKey() : aFirst.getValue().compareTo(aSecond.getValue())).collect(Collectors.toList());


        StringBuilder myOutput = new StringBuilder();
        for (Map.Entry<Integer, String> aWord : myWordsList) {
            myOutput.append(aWord.getKey()).append(" ").append(aWord.getValue()).append("\n");
        }

        try {
            Files.write(Paths.get(theCustomFTPServer.getHomeDirectory() + "/output.txt"), myOutput.toString().getBytes());
            SocketUtils.write(aClientSocket, "reduce 2 done");
            System.out.println("Reduce 2 done");
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }

    public void run() {
        Socket myClientSocket = null;
        try {
            myClientSocket = theServerSocket.accept();
        } catch (IOException aE) {
            aE.printStackTrace();
        }

        getNumberOfServersAndServerName(myClientSocket);
        if (theNumberOfServers == -1) {
            try {
                theServerSocket.close();
            } catch (IOException aE) {
                aE.printStackTrace();
            }
            System.out.println("Failed to get number of servers");
            return;
        }

        String[] myServers = getServerNames();

        StringBuilder[] myTokensList = mapPhase1();

        shufflePhase1(myServers, myTokensList, myClientSocket);

        Map<String, Integer> myWordCounts = reducePhase1(myClientSocket, myServers, myTokensList);

        shufflePhase2(myWordCounts, myServers, myClientSocket, getBoundaries(myWordCounts, myClientSocket), myTokensList);

        reducePhase2(myClientSocket, myTokensList, myServers);

        try {
            assert myClientSocket != null;
            myClientSocket.close();
            theServerSocket.close();
        } catch (IOException aE) {
            aE.printStackTrace();
        }
    }
}
