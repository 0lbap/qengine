package qengine.benchmark;

import qengine.model.StarQuery;
import qengine.parser.RDFAtomParser;
import qengine.parser.StarQuerySparQLParser;
import qengine.storage.RDFHexaStore;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class CleanQueryset {

    private final String datasetPath;
    private final String querysetDirPath;
    private final String outputDirPath;

    public CleanQueryset(String datasetPath, String querysetDirPath, String outputDirPath) {
        this.datasetPath = datasetPath;
        this.querysetDirPath = querysetDirPath;
        this.outputDirPath = outputDirPath;
    }

    public void supprimerDoublon() throws IOException {
        File dir = new File(querysetDirPath);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new IllegalArgumentException("Le répertoire des fichiers queryset n'existe pas ou n'est pas un dossier.");
        }

        File[] querysetFiles = dir.listFiles((d, name) -> name.endsWith(".queryset"));
        if (querysetFiles == null) {
            throw new IOException("Aucun fichier queryset trouvé dans le répertoire spécifié.");
        }

        int totalQueries = 0;
        int totalDuplicates = 0;

        for (File file : querysetFiles) {
            Set<StarQuery> uniqueQueries = new HashSet<>();
            List<StarQuery> allQueries = new ArrayList<>();

            try (StarQuerySparQLParser parser = new StarQuerySparQLParser(file.getAbsolutePath())) {
                while (parser.hasNext()) {
                    StarQuery query = (StarQuery) parser.next();
                    allQueries.add(query);

                    if (!uniqueQueries.add(query)) {
                        System.out.println("Doublon détecté : " + query.getLabel());
                        totalDuplicates++;
                    }

                }
            }

            totalQueries += allQueries.size();
            writeQueriesToFile(uniqueQueries, file.getName());
        }

        System.out.println("Nombre total de requêtes : " + totalQueries);
        System.out.println("Nombre total de doublons supprimés : " + totalDuplicates);
        System.out.println("Nombre total de requêtes restantes : " + (totalQueries - totalDuplicates));

    }

//    public void garderCinqPourcentDesRequeteZero() throws IOException {
//        RDFHexaStore store = loadDataset();
//        File[] querysetFiles = new File(outputDirPath).listFiles((d, name) -> name.endsWith(".queryset"));
//
//        if (querysetFiles == null) {
//            throw new FileNotFoundException("Aucun fichier queryset dédupliqué trouvé dans le répertoire : " + outputDirPath);
//        }
//
//        for (File file : querysetFiles) {
//            List<StarQuery> zeroResponseQueries = new ArrayList<>();
//            List<StarQuery> nonZeroResponseQueries = new ArrayList<>();
//
//            try (StarQuerySparQLParser parser = new StarQuerySparQLParser(file.getAbsolutePath())) {
//                while (parser.hasNext()) {
//                    StarQuery query = (StarQuery) parser.next();
//                    long responseCount = store.match(query).hasNext() ? 1 : 0;
//                    if (responseCount == 0) {
//                        zeroResponseQueries.add(query);
//                    } else {
//                        nonZeroResponseQueries.add(query);
//                    }
//                }
//            }
//
//            int zeroToKeep = (int) Math.ceil(zeroResponseQueries.size() * 0.05);
//            Collections.shuffle(zeroResponseQueries);
//            List<StarQuery> filteredZeroQueries = zeroResponseQueries.subList(0, Math.min(zeroToKeep, zeroResponseQueries.size()));
//
//            List<StarQuery> finalQueries = new ArrayList<>();
//            finalQueries.addAll(nonZeroResponseQueries);
//            finalQueries.addAll(filteredZeroQueries);
//
//            writeQueriesToFile(finalQueries, file.getName());
//        }
//    }

    private RDFHexaStore loadDataset() throws IOException {
        RDFHexaStore store = new RDFHexaStore();
        File datasetFile = new File(datasetPath);
        if (!datasetFile.exists()) {
            throw new FileNotFoundException("Fichier dataset introuvable : " + datasetPath);
        }

        try (FileReader reader = new FileReader(datasetFile)) {
            RDFAtomParser parser = new RDFAtomParser(reader, org.eclipse.rdf4j.rio.RDFFormat.NTRIPLES);
            while (parser.hasNext()) {
                store.add(parser.next());
            }
        }
        return store;
    }

    private void writeQueriesToFile(Collection<StarQuery> queries, String outputFileName) throws IOException {
        Path outputPath = Paths.get(outputDirPath, outputFileName);
        Files.createDirectories(outputPath.getParent());

        try (BufferedWriter writer = Files.newBufferedWriter(outputPath)) {
            for (StarQuery query : queries) {
                writer.write(query.getLabel());
                writer.newLine();
                writer.newLine();
            }
        }
    }

    public static void main(String[] args) {

        String datasetPath = "data/500K.nt";
        String querysetDirPath = "watdiv-mini-projet-partie-2/testsuite/queries";
        String outputDirPath = "watdiv-mini-projet-partie-2/testsuite/result_query";

        CleanQueryset cleaner = new CleanQueryset(datasetPath, querysetDirPath, outputDirPath);

        try {
            System.out.println("Début du nettoyage des fichiers queryset...");
            cleaner.supprimerDoublon();
            System.out.println("Suppression des doublons terminée.");
            System.out.println("Début de la suppression des 95% des requêtes zéro...");
//            cleaner.garderCinqPourcentDesRequeteZero();
            System.out.println("Nettoyage terminé. Fichiers générés dans : " + outputDirPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

