package qengine.program;

import org.eclipse.rdf4j.rio.RDFFormat;
import qengine.model.RDFAtom;
import qengine.parser.RDFAtomParser;
import qengine.storage.RDFHexaStore;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class Experiments {

    private static final String WORKING_DIR = "data/";
    private static final String SAMPLE_DATA_FILE = WORKING_DIR + "sample_data.nt";
    private static final String SAMPLE_QUERY_FILE = WORKING_DIR + "sample_query.queryset";

    public static void main(String[] args) throws IOException {
        System.out.println("Beginning experiments");

        System.out.println("=== Parsing RDF Data ===");
        List<RDFAtom> rdfAtoms = parseRDFData(SAMPLE_DATA_FILE);

        // Affiche les atomes RDF parsés
//        System.out.println("\n=== RDF Atoms ===");
//        for (RDFAtom atom : rdfAtoms) {
//            System.out.println("---");
//            System.out.println(atom.getTripleSubject());
//            System.out.println(atom.getTripleObject());
//            System.out.println(atom.getTriplePredicate());
//            System.out.println("---");
//        }

        System.out.println("Adding an element to the HexaStore");
        RDFHexaStore hs = new RDFHexaStore();
        hs.addAll(rdfAtoms);
        System.out.println(hs.toString());
    }

    private static List<RDFAtom> parseRDFData(String rdfFilePath) throws IOException {
        FileReader rdfFile = new FileReader(rdfFilePath);
        List<RDFAtom> rdfAtoms = new ArrayList<>();

        try (RDFAtomParser rdfAtomParser = new RDFAtomParser(rdfFile, RDFFormat.NTRIPLES)) {
            int count = 0;
            while (rdfAtomParser.hasNext()) {
                RDFAtom atom = rdfAtomParser.next();
                rdfAtoms.add(atom);  // Stocker l'atome dans la collection
                System.out.println("RDF Atom #" + (++count) + ": " + atom);
            }
            System.out.println("Total RDF Atoms parsed: " + count);
        }
        return rdfAtoms;
    }

}
