package qengine.benchmark;

import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;
import qengine.storage.RDFHexaStore;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class HexastoreBenchmark {

    private static String dataFilePath;
    private static String querysetDirPath;
    private static String outputFilePath;

    public static void start(String dataFilePath, String querysetDirPath, String outputFilePath) throws IOException {
        HexastoreBenchmark.dataFilePath = dataFilePath;
        HexastoreBenchmark.querysetDirPath = querysetDirPath;
        HexastoreBenchmark.outputFilePath = outputFilePath;
        HexastoreBenchmark.main(new String[]{});
    }

    public static void main(String[] args) throws IOException {
        List<RDFAtom> rdfAtoms = parseRDFData(dataFilePath);

        RDFHexaStore store = new RDFHexaStore();
        store.addAll(rdfAtoms);

        System.out.println("Données RDF chargées dans le store. Début du benchmark...");

        Map<String, Long> results = executeGroupedQueries(querysetDirPath, store);
        saveResultsToFile(results);

        System.out.println("Benchmarks terminés. Résultats enregistrés dans le répertoire 'benchmark'.");
    }

    private static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
        FileReader rdfFile = new FileReader(rdfFilePath);
        List<RDFAtom> rdfAtoms = new ArrayList<>();

        try (RDFAtomParser rdfAtomParser = new RDFAtomParser(rdfFile, RDFFormat.NTRIPLES)) {
            while (rdfAtomParser.hasNext()) {
                rdfAtoms.add(rdfAtomParser.next());
            }
        }
        return rdfAtoms;
    }

    private static Map<String, Long> executeGroupedQueries(String querySetDir, RDFHexaStore store) {
        Map<String, Long> groupedResults = new TreeMap<>();
        File dir = new File(querySetDir);

        if (!dir.exists() || !dir.isDirectory()) {
            System.err.println("Le répertoire spécifié n'existe pas : " + querySetDir);
            return groupedResults;
        }

        File[] queryFiles = dir.listFiles((d, name) -> name.endsWith(".queryset"));
        if (queryFiles == null) {
            System.err.println("Aucun fichier queryset trouvé dans : " + querySetDir);
            return groupedResults;
        }

        Map<String, List<File>> groupedFiles = new TreeMap<>();
        for (File queryFile : queryFiles) {
            String fileName = queryFile.getName();
            String[] parts = fileName.split("_");
            String category = parts[0] + parts[1];

            groupedFiles.computeIfAbsent(category, k -> new ArrayList<>()).add(queryFile);
        }

        for (Map.Entry<String, List<File>> entry : groupedFiles.entrySet()) {
            String category = entry.getKey();
            List<File> files = entry.getValue();

            long startTime = System.currentTimeMillis();
            for (File file : files) {
                executeAllQueriesFromFile(file, store);
            }
            long totalTime = System.currentTimeMillis() - startTime;

            groupedResults.put(category, totalTime);
        }

        return groupedResults;
    }

    private static void executeAllQueriesFromFile(File queryFile, RDFHexaStore store) {
        try (StarQuerySparQLParser parser = new StarQuerySparQLParser(queryFile.getAbsolutePath())) {
            while (parser.hasNext()) {
                StarQuery query = (StarQuery) parser.next();
                store.match(query);
            }
        } catch (IOException e) {
            System.err.println("Erreur lors du traitement du fichier : " + queryFile.getName());
        }
    }

    private static void saveResultsToFile(Map<String, Long> results) {
        try (FileWriter writer = new FileWriter(outputFilePath)) {
            writer.write("=== MACHINE ===\n");
            writer.write(MachineInfo.getMachineInfo());
            writer.write("\n");

            for (Map.Entry<String, Long> entry : results.entrySet()) {
                writer.write("=== " + entry.getKey() + " ===\n");
                writer.write("TOTAL : " + entry.getValue() + "ms\n\n");
            }
            System.out.println("Résultats sauvegardés dans : " + outputFilePath);
        } catch (IOException e) {
            System.err.println("Erreur lors de l'écriture du fichier de benchmark : " + outputFilePath);
        }
    }
}
