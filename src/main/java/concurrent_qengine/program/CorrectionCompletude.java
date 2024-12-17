package concurrent_qengine.program;

import fr.boreal.model.formula.api.FOFormula;
import fr.boreal.model.formula.api.FOFormulaConjunction;
import fr.boreal.model.kb.api.FactBase;
import fr.boreal.model.logicalElements.api.Substitution;
import fr.boreal.model.query.api.FOQuery;
import fr.boreal.model.query.api.Query;
import fr.boreal.model.queryEvaluation.api.FOQueryEvaluator;
import fr.boreal.query_evaluation.generic.GenericFOQueryEvaluator;
import fr.boreal.storage.natives.SimpleInMemoryGraphStore;
import org.eclipse.rdf4j.rio.RDFFormat;
import concurrent_qengine.model.RDFAtom;
import concurrent_qengine.model.StarQuery;
import concurrent_qengine.parser.RDFAtomParser;
import concurrent_qengine.parser.StarQuerySparQLParser;
import concurrent_qengine.storage.RDFHexaStore;

import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Path;
import java.util.*;
import java.nio.file.Files;
import java.nio.file.Paths;



import java.io.IOException;
import java.util.stream.Stream;

public final class CorrectionCompletude {

	static TreeMap<Integer, Integer> resultsnb = new TreeMap();
	static int compteurCorrect = 0;
	static int compteurIncorrect = 0;



	public static void main(String[] args) throws IOException {
		int qteData = 5;
		String rdfFilePath = "data/data" + qteData + ".nt";
		String queryDirPath = "data/queries";
		String outputDir = "results/";
		String outputDirErr = "results_errors/";

		// Charger les données RDF
		List<RDFAtom> rdfAtoms = parseRDFData(rdfFilePath);
		RDFHexaStore hexaStore = new RDFHexaStore();
		FactBase factBase = new SimpleInMemoryGraphStore();

		for (RDFAtom atom : rdfAtoms) {
			hexaStore.add(atom);
			factBase.add(atom);
		}

		// On prends tous les fichiers du dossier query
		try (Stream<Path> paths = Files.walk(Paths.get(queryDirPath))) {
			paths.filter(Files::isRegularFile)
					.filter(path -> path.toString().endsWith(".queryset"))
					.forEach(path -> {
						try {
							//System.out.println("Processing query file: " + path);
							List<StarQuery> starQueries = parseSparQLQueries(path.toString());
							compareResults(hexaStore, factBase, starQueries, outputDir, outputDirErr);
						} catch (IOException e) {
							System.err.println("Error processing file " + path + ": " + e.getMessage());
						}
					});
		} catch (IOException e) {
			System.err.println("Error accessing query files: " + e.getMessage());
		}


		System.out.println("correct " + compteurCorrect);
		System.out.println("incorrect " + compteurIncorrect);


		String csvFile = "nbresults" + qteData + ".csv";


		try (FileWriter writer = new FileWriter(csvFile)) {
			// Écrire les en-têtes
			writer.append("Nb de réponses,queries\n");

			for (Integer key : resultsnb.keySet()) {
				writer.append(key.toString())
						.append(",")
						.append(resultsnb.get(key).toString())
						.append("\n");
			}

			System.out.println("CSV créé avec succès : " + csvFile);
		} catch (IOException e) {
			System.err.println("Erreur lors de l'écriture du fichier : " + e.getMessage());
		}
	}


	private static void compareResults(RDFHexaStore hexaStore, FactBase factBase, List<StarQuery> starQueries, String outputDir, String outputDirErr) throws IOException {
		for (StarQuery query : starQueries) {
			//System.out.printf("Executing query: %s%n", query);

			// Résultats HexaStore implémenté
			Set<Substitution> hexaResults = new HashSet<>();
			hexaStore.match(query).forEachRemaining(hexaResults::add);

			// Résultats d'Integraal
			FOQuery<FOFormulaConjunction> foQuery = query.asFOQuery();
			FOQueryEvaluator<FOFormula> evaluator = GenericFOQueryEvaluator.defaultInstance();
			Iterator<Substitution> integraalResultsIter = evaluator.evaluate(foQuery, factBase);
			Set<Substitution> integraalResults = new HashSet<>();
			integraalResultsIter.forEachRemaining(integraalResults::add);

			// Comparaison
			boolean isCorrect = hexaResults.equals(integraalResults);
			boolean isComplete = integraalResults.containsAll(hexaResults);

			//System.out.printf("Query: %s%n", query);
			//System.out.printf("Correct: %s, Complete: %s%n", isCorrect, isComplete);

			// Exporter les résultats
			if (!isCorrect || !isComplete ) {
				compteurIncorrect++;
				exportResults(query, hexaResults, integraalResults, isCorrect, isComplete, outputDirErr);
			} else {
				exportResults(query, hexaResults, integraalResults, isCorrect, isComplete, outputDir);
				compteurCorrect++;
			}
		}
	}

	private static void exportResults(StarQuery query, Set<Substitution> hexaResults, Set<Substitution> integraalResults, boolean isCorrect, boolean isComplete, String outputDir) throws IOException {
		String filename = outputDir + "query_" + query.hashCode() + ".txt";
		Files.createDirectories(Paths.get(outputDir));
		StringBuilder output = new StringBuilder();

		output.append("Query: ").append(query).append("\n");
		output.append("Correct: ").append(isCorrect).append("\n");
		output.append("Complete: ").append(isComplete).append("\n\n");
		output.append("HexaStore Results:\n");
		hexaResults.forEach(result -> output.append(result).append("\n"));
		output.append("\nIntegraal Results:\n");
		integraalResults.forEach(result -> output.append(result).append("\n"));

		//Files.writeString(Paths.get(filename), output.toString());
		//System.out.println("Results exported to " + filename);

		int nbAnswers = hexaResults.size();
		if(resultsnb.get(nbAnswers) == null) {
			resultsnb.put(nbAnswers, 1);
		} else {
			resultsnb.put(nbAnswers, resultsnb.get(nbAnswers) + 1);
		}
	}

	private static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
		FileReader rdfFile = new FileReader(rdfFilePath);
		List<RDFAtom> rdfAtoms = new ArrayList<>();

		try (RDFAtomParser rdfAtomParser = new RDFAtomParser(rdfFile, RDFFormat.NTRIPLES)) {
			int count = 0;
			while (rdfAtomParser.hasNext()) {
				RDFAtom atom = rdfAtomParser.next();
				rdfAtoms.add(atom);
				count++;
				//System.out.println("RDF Atom #" + (++count) + ": " + atom);
			}
			System.out.println("Total RDF Atoms parsed: " + count);
		}
		return rdfAtoms;
	}

	/**
	 * Parse et affiche le contenu d'un fichier de requêtes SparQL.
	 *
	 * @param queryFilePath Chemin vers le fichier de requêtes SparQL
	 * @return Liste des StarQueries parsées
	 */
	private static List<StarQuery> parseSparQLQueries(String queryFilePath) throws IOException {
		List<StarQuery> starQueries = new ArrayList<>();

		try (StarQuerySparQLParser queryParser = new StarQuerySparQLParser(queryFilePath)) {
			int queryCount = 0;

			while (queryParser.hasNext()) {
				Query query = queryParser.next();
				if (query instanceof StarQuery starQuery) {
					starQueries.add(starQuery);  // Stocker la requête dans la collection
					queryCount++;
					/*
					System.out.println("Star Query #" + (++queryCount) + ":");
					System.out.println("  Central Variable: " + starQuery.getCentralVariable().label());
					System.out.println("  RDF Atoms:");
					starQuery.getRdfAtoms().forEach(atom -> System.out.println("    " + atom));
				*/
				} else {
					System.err.println("Requête inconnue ignorée.");
				}
			}
			//System.out.println("Total Queries parsed: " + starQueries.size());
		}
		return starQueries;
	}
}
