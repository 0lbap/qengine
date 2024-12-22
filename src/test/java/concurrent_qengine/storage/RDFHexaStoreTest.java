package concurrent_qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.factory.impl.SameObjectTermFactory;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import concurrent_qengine.model.RDFAtom;
import concurrent_qengine.model.StarQuery;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests unitaires pour la classe {@link RDFHexaStore}.
 */
public class RDFHexaStoreTest {
    private static final Literal<String> SUBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("subject1");
    private static final Literal<String> PREDICATE_1 = SameObjectTermFactory.instance().createOrGetLiteral("predicate1");
    private static final Literal<String> OBJECT_1 = SameObjectTermFactory.instance().createOrGetLiteral("object1");
    private static final Literal<String> SUBJECT_2 = SameObjectTermFactory.instance().createOrGetLiteral("subject2");
    private static final Literal<String> SUBJECT_3 = SameObjectTermFactory.instance().createOrGetLiteral("subject3");
    private static final Literal<String> PREDICATE_2 = SameObjectTermFactory.instance().createOrGetLiteral("predicate2");
    private static final Literal<String> PREDICATE_3 = SameObjectTermFactory.instance().createOrGetLiteral("predicate3");
    private static final Literal<String> OBJECT_2 = SameObjectTermFactory.instance().createOrGetLiteral("object2");
    private static final Literal<String> OBJECT_3 = SameObjectTermFactory.instance().createOrGetLiteral("object3");
    private static final Variable VAR_X = SameObjectTermFactory.instance().createOrGetVariable("?x");
    private static final Variable VAR_Y = SameObjectTermFactory.instance().createOrGetVariable("?y");
    private static final Variable VAR_Z = SameObjectTermFactory.instance().createOrGetVariable("?z");

    @Test
    public void testAddAllRDFAtoms() {
        RDFHexaStore store = new RDFHexaStore();

        // Version stream
        // Ajouter plusieurs RDFAtom
        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        Set<RDFAtom> rdfAtoms = Set.of(rdfAtom1, rdfAtom2);

        assertTrue(store.addAll(rdfAtoms.stream()), "Les RDFAtoms devraient être ajoutés avec succès.");

        // Vérifier que tous les atomes sont présents
        Collection<Atom> atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom1), "La base devrait contenir le premier RDFAtom ajouté.");
        assertTrue(atoms.contains(rdfAtom2), "La base devrait contenir le second RDFAtom ajouté.");

        // Version collection
        store = new RDFHexaStore();
        assertTrue(store.addAll(rdfAtoms), "Les RDFAtoms devraient être ajoutés avec succès.");

        // Vérifier que tous les atomes sont présents
        atoms = store.getAtoms();
        assertTrue(atoms.contains(rdfAtom1), "La base devrait contenir le premier RDFAtom ajouté.");
        assertTrue(atoms.contains(rdfAtom2), "La base devrait contenir le second RDFAtom ajouté.");
    }




    @Test
    public void testAddRDFAtom() {

        RDFHexaStore store = new RDFHexaStore();

        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        store.add(rdfAtom1);
        store.add(rdfAtom2);

        // Vérifier le size
        assertEquals(2,store.size(), "La taille du store doit être de 2");


        // Vérifier que les valeurs soient bien dans dic_int_term :
        assertTrue(store.dic_int_term.containsValue(SUBJECT_1), "Le dictionnaire devrait contenir SUBJECT_1.");
        assertTrue(store.dic_int_term.containsValue(PREDICATE_1), "Le dictionnaire devrait contenir PREDICATE_1.");
        assertTrue(store.dic_int_term.containsValue(OBJECT_1), "Le dictionnaire devrait contenir OBJECT_1.");
        assertTrue(store.dic_int_term.containsValue(SUBJECT_2), "Le dictionnaire devrait contenir SUBJECT_2.");
        assertTrue(store.dic_int_term.containsValue(PREDICATE_2), "Le dictionnaire devrait contenir PREDICATE_2.");
        assertTrue(store.dic_int_term.containsValue(OBJECT_2), "Le dictionnaire devrait contenir OBJECT_2.");

        // Vérifier les clés pour chaque terme :
        assertEquals(SUBJECT_1, store.dic_int_term.get(1), "Le premier élément devrait être SUBJECT_1.");
        assertEquals(PREDICATE_1, store.dic_int_term.get(2), "Le deuxième élément devrait être PREDICATE_1.");
        assertEquals(OBJECT_1, store.dic_int_term.get(3), "Le troisième élément devrait être OBJECT_1.");
        assertEquals(SUBJECT_2, store.dic_int_term.get(4), "Le quatrième élément devrait être SUBJECT_2.");
        assertEquals(PREDICATE_2, store.dic_int_term.get(5), "Le cinquième élément devrait être PREDICATE_2.");
        assertEquals(OBJECT_2, store.dic_int_term.get(6), "Le sixième élément devrait être OBJECT_2.");


        int subject1Id = store.dic_term_int.get(SUBJECT_1);
        int predicate1Id = store.dic_term_int.get(PREDICATE_1);
        int object1Id = store.dic_term_int.get(OBJECT_1);

        int subject2Id = store.dic_term_int.get(SUBJECT_2);
        int predicate2Id = store.dic_term_int.get(PREDICATE_2);
        int object2Id = store.dic_term_int.get(OBJECT_2);

        // Vérifier SPO
        assertTrue(store.spo.containsKey(subject1Id), "L'index SPO devrait contenir subject1Id.");
        assertTrue(store.spo.get(subject1Id).containsKey(predicate1Id), "L'index SPO devrait contenir predicate1Id pour subject1Id.");
        assertTrue(store.spo.get(subject1Id).get(predicate1Id).contains(object1Id), "L'index SPO devrait contenir object1Id pour (subject1Id, predicate1Id).");

        assertTrue(store.spo.containsKey(subject2Id), "L'index SPO devrait contenir subject2Id.");
        assertTrue(store.spo.get(subject2Id).containsKey(predicate2Id), "L'index SPO devrait contenir predicate2Id pour subject2Id.");
        assertTrue(store.spo.get(subject2Id).get(predicate2Id).contains(object2Id), "L'index SPO devrait contenir object2Id pour (subject2Id, predicate2Id).");

        // Vérifier SOP
        assertTrue(store.sop.containsKey(subject1Id), "L'index SOP devrait contenir subject1Id.");
        assertTrue(store.sop.get(subject1Id).containsKey(object1Id), "L'index SOP devrait contenir object1Id pour subject1Id.");
        assertTrue(store.sop.get(subject1Id).get(object1Id).contains(predicate1Id), "L'index SOP devrait contenir predicate1Id pour (subject1Id, object1Id).");

        assertTrue(store.sop.containsKey(subject2Id), "L'index SOP devrait contenir subject2Id.");
        assertTrue(store.sop.get(subject2Id).containsKey(object2Id), "L'index SOP devrait contenir object2Id pour subject2Id.");
        assertTrue(store.sop.get(subject2Id).get(object2Id).contains(predicate2Id), "L'index SOP devrait contenir predicate2Id pour (subject2Id, predicate2Id).");

        // Vérifier POS
        assertTrue(store.pos.containsKey(predicate1Id), "L'index POS devrait contenir predicate1Id.");
        assertTrue(store.pos.get(predicate1Id).containsKey(object1Id), "L'index POS devrait contenir object1Id pour predicate1Id.");
        assertTrue(store.pos.get(predicate1Id).get(object1Id).contains(subject1Id), "L'index POS devrait contenir subject1Id pour (predicate1Id, object1Id).");

        assertTrue(store.pos.containsKey(predicate2Id), "L'index POS devrait contenir predicate2Id.");
        assertTrue(store.pos.get(predicate2Id).containsKey(object2Id), "L'index POS devrait contenir object2Id pour predicate2Id.");
        assertTrue(store.pos.get(predicate2Id).get(object2Id).contains(subject2Id), "L'index POS devrait contenir subject2Id pour (predicate2Id, object2Id).");

        // Vérifier PSO
        assertTrue(store.pso.containsKey(predicate1Id), "L'index PSO devrait contenir predicate1Id.");
        assertTrue(store.pso.get(predicate1Id).containsKey(subject1Id), "L'index PSO devrait contenir subject1Id pour predicate1Id.");
        assertTrue(store.pso.get(predicate1Id).get(subject1Id).contains(object1Id), "L'index PSO devrait contenir object1Id pour (predicate1Id, subject1Id).");

        assertTrue(store.pso.containsKey(predicate2Id), "L'index PSO devrait contenir predicate2Id.");
        assertTrue(store.pso.get(predicate2Id).containsKey(subject2Id), "L'index PSO devrait contenir subject2Id pour predicate2Id.");
        assertTrue(store.pso.get(predicate2Id).get(subject2Id).contains(object2Id), "L'index PSO devrait contenir object2Id pour (predicate2Id, subject2Id).");

        // Vérifier OPS
        assertTrue(store.ops.containsKey(object1Id), "L'index OPS devrait contenir object1Id.");
        assertTrue(store.ops.get(object1Id).containsKey(predicate1Id), "L'index OPS devrait contenir predicate1Id pour object1Id.");
        assertTrue(store.ops.get(object1Id).get(predicate1Id).contains(subject1Id), "L'index OPS devrait contenir subject1Id pour (object1Id, predicate1Id).");

        assertTrue(store.ops.containsKey(object2Id), "L'index OPS devrait contenir object2Id.");
        assertTrue(store.ops.get(object2Id).containsKey(predicate2Id), "L'index OPS devrait contenir predicate2Id pour object2Id.");
        assertTrue(store.ops.get(object2Id).get(predicate2Id).contains(subject2Id), "L'index OPS devrait contenir subject2Id pour (object2Id, predicate2Id).");

        // Vérifier OSP
        assertTrue(store.osp.containsKey(object1Id), "L'index OSP devrait contenir object1Id.");
        assertTrue(store.osp.get(object1Id).containsKey(subject1Id), "L'index OSP devrait contenir subject1Id pour object1Id.");
        assertTrue(store.osp.get(object1Id).get(subject1Id).contains(predicate1Id), "L'index OSP devrait contenir predicate1Id pour (object1Id, subject1Id).");

        assertTrue(store.osp.containsKey(object2Id), "L'index OSP devrait contenir object2Id.");
        assertTrue(store.osp.get(object2Id).containsKey(subject2Id), "L'index OSP devrait contenir subject2Id pour object2Id.");
        assertTrue(store.osp.get(object2Id).get(subject2Id).contains(predicate2Id), "L'index OSP devrait contenir predicate2Id pour (object2Id, subject2Id).");

    }




    @Test
    public void testAddDuplicateAtom() {
        RDFHexaStore store = new RDFHexaStore();

        // Ajouter 2 fois le même RDFAtom
        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);

        store.add(rdfAtom1);
        store.add(rdfAtom1);

        // Vérifier que les valeurs soient bien dans dic :
        assertTrue(store.dic_int_term.containsValue(SUBJECT_1), "Le dictionnaire devrait contenir SUBJECT_1.");
        assertTrue(store.dic_int_term.containsValue(PREDICATE_1), "Le dictionnaire devrait contenir PREDICATE_1.");
        assertTrue(store.dic_int_term.containsValue(OBJECT_1), "Le dictionnaire devrait contenir OBJECT_1.");

        // Vérifier les clés pour chaque terme :
        assertEquals(SUBJECT_1, store.dic_int_term.get(1), "Le premier élément devrait être SUBJECT_1.");
        assertEquals(PREDICATE_1, store.dic_int_term.get(2), "Le deuxième élément devrait être PREDICATE_1.");
        assertEquals(OBJECT_1, store.dic_int_term.get(3), "Le troisième élément devrait être OBJECT_1.");
    }




    @Test
    public void testSize() {
        RDFHexaStore store = new RDFHexaStore();
        RDFAtom rdfAtom1 = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2);

        // Ajouter 2 fois le même RDFAtom
        store.add(rdfAtom1);
        store.add(rdfAtom1);

        // Vérifier la taille des dictionnaires = 3
        assertEquals(3,store.dic_int_term.size(), "Le dictionnaire entiers vers termes devrait contenir trois éléments.");
        assertEquals(3,store.dic_term_int.size(), "Le dictionnaire termes vers entiers devrait contenir trois éléments.");

        store.add(rdfAtom2);

        // Vérifier la taille des dictionnaires = 6
        assertEquals(6,store.dic_int_term.size(), "Le dictionnaire entiers vers termes devrait contenir six éléments.");
        assertEquals(6,store.dic_term_int.size(), "Le dictionnaire termes vers entiers devrait contenir six éléments.");

        RDFAtom query2 = new RDFAtom(VAR_X, PREDICATE_2, OBJECT_1);
        Iterator<Substitution> matches2 = store.match(query2);
        List<Substitution> results2 = new ArrayList<>();
        matches2.forEachRemaining(results2::add);
        assertEquals(0, results2.size(), "Zéro correspondance devrait être trouvée.");
    }




    @Test
    public void testMatchAtomSVariable() {
        RDFHexaStore store = new RDFHexaStore();

        // Ajouter des triplets RDF
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_2));

        RDFAtom query = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1);
        Iterator<Substitution> matches = store.match(query);

        List<Substitution> results = new ArrayList<>();
        matches.forEachRemaining(results::add);

        // substitutions attendues :
        Substitution expected1 = new SubstitutionImpl();
        expected1.add(VAR_X, SUBJECT_1);
        Substitution expected2 = new SubstitutionImpl();
        expected2.add(VAR_X, SUBJECT_2);

        // Vérifier les résultats
        assertEquals(2, results.size(), "Deux correspondances devraient être trouvées.");
        assertTrue(results.contains(expected1), "La substitution VAR_X -> SUBJECT_1 devrait être présente.");
        assertTrue(results.contains(expected2), "La substitution VAR_X -> SUBJECT_2 devrait être présente.");

        // Tests bloquants
        RDFAtom query2 = new RDFAtom(VAR_X, PREDICATE_2, OBJECT_1);
        Iterator<Substitution> matches2 = store.match(query2);
        List<Substitution> results2 = new ArrayList<>();
        matches2.forEachRemaining(results2::add);
        assertEquals(0, results2.size(), "Zéro correspondance devrait être trouvée.");


    }




    @Test
    public void testMatchAtomOVariable() {
        RDFHexaStore store = new RDFHexaStore();

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_2));

        RDFAtom query = new RDFAtom(SUBJECT_1, PREDICATE_1, VAR_X);
        Iterator<Substitution> matches = store.match(query);

        List<Substitution> results = new ArrayList<>();
        matches.forEachRemaining(results::add);

        Substitution expected1 = new SubstitutionImpl();
        expected1.add(VAR_X, OBJECT_1);
        Substitution expected2 = new SubstitutionImpl();
        expected2.add(VAR_X, OBJECT_2);

        // Vérifier les résultats
        assertEquals(2, results.size(), "Deux correspondances devraient être trouvées.");
        assertTrue(results.contains(expected1), "La substitution VAR_X -> OBJECT_1 devrait être présente.");
        assertTrue(results.contains(expected2), "La substitution VAR_X -> OBJECT_2 devrait être présente.");

        RDFAtom query2 = new RDFAtom(SUBJECT_3, PREDICATE_2, VAR_X);
        Iterator<Substitution> matches2 = store.match(query2);
        List<Substitution> results2 = new ArrayList<>();
        matches2.forEachRemaining(results2::add);
        assertEquals(0, results2.size(), "Zéro correspondance devrait être trouvée.");

    }




    @Test
    public void testMatchAtomPVariable() {
        RDFHexaStore store = new RDFHexaStore();

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_2));

        RDFAtom query = new RDFAtom(SUBJECT_1, VAR_X, OBJECT_2);
        Iterator<Substitution> matches = store.match(query);

        List<Substitution> results = new ArrayList<>();
        matches.forEachRemaining(results::add);

        Substitution expected1 = new SubstitutionImpl();
        expected1.add(VAR_X, PREDICATE_1);

        // Vérifier les résultats
        assertEquals(1, results.size(), "Une correspondance devrait être trouvée.");
        assertTrue(results.contains(expected1), "La substitution VAR_X -> PREDICATE_1 devrait être présente.");

        // Test Bloquant
        RDFAtom query2 = new RDFAtom(SUBJECT_1, VAR_Y, OBJECT_3);
        Iterator<Substitution> matches2 = store.match(query2);
        List<Substitution> results2 = new ArrayList<>();
        matches2.forEachRemaining(results2::add);
        assertEquals(0, results2.size(), "Zéro correspondance devrait être trouvée.");

    }




    @Test
    public void testMatchStarQuery() {
        RDFHexaStore store = new RDFHexaStore();

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_3));
        store.add(new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_3));



        RDFAtom rdfAtom1 = new RDFAtom(VAR_X, PREDICATE_2, OBJECT_1);
        RDFAtom rdfAtom2 = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_3);
        List<RDFAtom> rdfAtoms = new ArrayList<>();
        rdfAtoms.add(rdfAtom1);
        rdfAtoms.add(rdfAtom2);

        Collection<Variable> answerVariables = new ArrayList<>();
        answerVariables.add(VAR_X);

        StarQuery starQuery = new StarQuery("subject1", rdfAtoms, answerVariables );
        Iterator<Substitution> matches = store.match(starQuery);

        List<Substitution> results = new ArrayList<>();
        matches.forEachRemaining(results::add);

        Substitution expected1 = new SubstitutionImpl();
        expected1.add(VAR_X, SUBJECT_1);


        assertEquals(1, results.size(), "Une correspondance devrait être trouvée.");
        assertTrue(results.contains(expected1), "La substitution VAR_X -> SUBJECT_1 devrait être présente.");

        // Test avec une requete plus importante :
        // ?x predicat1 objet1
        // ?x predicat2 objet2
        // ?x predicat3 objet3
        // dans la base :
        // subject1 predicat1 objet1
        // subject2 predicat1 objet1
        // subject3 predicat1 objet1
        // subject1 predicat2 objet2
        // subject2 predicat2 objet2
        // subject1 predicat3 objet3

        RDFHexaStore store2 = new RDFHexaStore();

        store2.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store2.add(new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_1));
        store2.add(new RDFAtom(SUBJECT_3, PREDICATE_1, OBJECT_1));
        store2.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_2));
        store2.add(new RDFAtom(SUBJECT_2, PREDICATE_2, OBJECT_2));
        store2.add(new RDFAtom(SUBJECT_1, PREDICATE_3, OBJECT_3));



        RDFAtom rdfAtom3 = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom4 = new RDFAtom(VAR_X, PREDICATE_2, OBJECT_2);
        RDFAtom rdfAtom5 = new RDFAtom(VAR_X, PREDICATE_3, OBJECT_3);
        List<RDFAtom> rdfAtoms2 = new ArrayList<>();
        rdfAtoms2.add(rdfAtom3);
        rdfAtoms2.add(rdfAtom4);
        rdfAtoms2.add(rdfAtom5);

        Collection<Variable> answerVariables2 = new ArrayList<>();
        answerVariables2.add(VAR_X);

        StarQuery starQuery2 = new StarQuery("subject1", rdfAtoms2, answerVariables2 );
        Iterator<Substitution> matches2 = store2.match(starQuery2);

        List<Substitution> results2 = new ArrayList<>();
        matches2.forEachRemaining(results2::add);

        Substitution expected2 = new SubstitutionImpl();
        expected2.add(VAR_X, SUBJECT_1);


        assertEquals(1, results2.size(), "Une correspondance devrait être trouvée.");
        assertTrue(results2.contains(expected2), "La substitution VAR_X -> SUBJECT_1 devrait être présente.");

        // Test Bloquant

        // Test avec une requete plus importante :
        // ?x predicat1 objet1
        // ?x predicat2 objet3
        // ?x predicat3 objet3
        // dans la base :
        // subject1 predicat1 objet1
        // subject2 predicat1 objet1
        // subject3 predicat1 objet1
        // subject1 predicat2 objet2
        // subject2 predicat2 objet2
        // subject1 predicat3 objet3

        RDFAtom rdfAtom6 = new RDFAtom(VAR_X, PREDICATE_1, OBJECT_1);
        RDFAtom rdfAtom7 = new RDFAtom(VAR_X, PREDICATE_2, OBJECT_3);
        RDFAtom rdfAtom8 = new RDFAtom(VAR_X, PREDICATE_3, OBJECT_3);
        List<RDFAtom> rdfAtoms3 = new ArrayList<>();
        rdfAtoms3.add(rdfAtom6);
        rdfAtoms3.add(rdfAtom7);
        rdfAtoms3.add(rdfAtom8);

        Collection<Variable> answerVariables3 = new ArrayList<>();
        answerVariables3.add(VAR_X);

        StarQuery starQuery3 = new StarQuery("subject1", rdfAtoms3, answerVariables3);
        Iterator<Substitution> matches3 = store2.match(starQuery3);

        List<Substitution> results3 = new ArrayList<>();
        matches3.forEachRemaining(results3::add);

        assertEquals(0, results3.size(), "Zéro correspondance devrait être trouvée.");



    }




    @Test
    public void testMatchPOVariables() {
        RDFHexaStore store = new RDFHexaStore();

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_2));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_3));

        RDFAtom query = new RDFAtom(SUBJECT_1, VAR_Y, VAR_X);
        Iterator<Substitution> matches = store.match(query);

        List<Substitution> results = new ArrayList<>();
        matches.forEachRemaining(results::add);

        // Substitutions attendues
        Substitution expected1 = new SubstitutionImpl();
        expected1.add(VAR_Y, PREDICATE_1);
        expected1.add(VAR_X, OBJECT_1);

        Substitution expected2 = new SubstitutionImpl();
        expected2.add(VAR_Y, PREDICATE_2);
        expected2.add(VAR_X, OBJECT_2);

        Substitution expected3 = new SubstitutionImpl();
        expected3.add(VAR_Y, PREDICATE_2);
        expected3.add(VAR_X, OBJECT_3);

        // Vérifier les résultats
        assertEquals(3, results.size(), "Trois correspondances devraient être trouvées.");
        assertTrue(results.contains(expected1), "La substitution VAR_Y -> PREDICATE_1, VAR_X -> OBJECT_1 devrait être présente.");
        assertTrue(results.contains(expected2), "La substitution VAR_Y -> PREDICATE_2, VAR_X -> OBJECT_2 devrait être présente.");
        assertTrue(results.contains(expected3), "La substitution VAR_Y -> PREDICATE_2, VAR_X -> OBJECT_3 devrait être présente.");

        // Test Bloquant
        RDFAtom query2 = new RDFAtom(SUBJECT_2, VAR_Y, VAR_X);
        Iterator<Substitution> matches2 = store.match(query2);
        List<Substitution> results2 = new ArrayList<>();
        matches2.forEachRemaining(results2::add);
        assertEquals(0, results2.size(), "Zéro correspondance devrait être trouvée.");
    }



    @Test
    public void testMatchSPVariables() {
        RDFHexaStore store = new RDFHexaStore();

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_2));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_1));

        RDFAtom query = new RDFAtom(VAR_X, VAR_Y, OBJECT_1);
        Iterator<Substitution> matches = store.match(query);

        List<Substitution> results = new ArrayList<>();
        matches.forEachRemaining(results::add);

        // Substitutions attendues
        Substitution expected1 = new SubstitutionImpl();
        expected1.add(VAR_X, SUBJECT_1);
        expected1.add(VAR_Y, PREDICATE_1);

        Substitution expected2 = new SubstitutionImpl();
        expected2.add(VAR_X, SUBJECT_1);
        expected2.add(VAR_Y, PREDICATE_2);


        // Vérifier les résultats
        assertEquals(2, results.size(), "Deux correspondances devraient être trouvées.");
        assertTrue(results.contains(expected1), "La substitution VAR_X -> SUBJECT_1, VAR_Y -> PREDICATE_1 devrait être présente.");
        assertTrue(results.contains(expected2), "La substitution VAR_X -> SUBJECT_1, VAR_Y -> PREDICATE_2 devrait être présente.");

        // Test Bloquant
        RDFAtom query2 = new RDFAtom(VAR_X, VAR_Y, OBJECT_3);
        Iterator<Substitution> matches2 = store.match(query2);
        List<Substitution> results2 = new ArrayList<>();
        matches2.forEachRemaining(results2::add);
        assertEquals(0, results2.size(), "Zéro correspondance devrait être trouvée.");
    }


    @Test
    public void testMatchSOVariables() {
        RDFHexaStore store = new RDFHexaStore();

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_2));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_3));

        RDFAtom query = new RDFAtom(VAR_X, PREDICATE_2, VAR_Y);
        Iterator<Substitution> matches = store.match(query);

        List<Substitution> results = new ArrayList<>();
        matches.forEachRemaining(results::add);

        // Substitutions attendues
        Substitution expected1 = new SubstitutionImpl();
        expected1.add(VAR_X, SUBJECT_1);
        expected1.add(VAR_Y, OBJECT_2);

        Substitution expected2 = new SubstitutionImpl();
        expected2.add(VAR_X, SUBJECT_1);
        expected2.add(VAR_Y, OBJECT_3);


        // Vérifier les résultats
        assertEquals(2, results.size(), "Deux correspondances devraient être trouvées.");
        assertTrue(results.contains(expected1), "La substitution VAR_X -> SUBJECT_1, VAR_Y -> OBJECT_2 devrait être présente.");
        assertTrue(results.contains(expected2), "La substitution VAR_X -> SUBJECT_1, VAR_Y -> OBJECT_3 devrait être présente.");


        // Test Bloquant
        RDFAtom query2 = new RDFAtom(VAR_X, PREDICATE_3, VAR_Y);
        Iterator<Substitution> matches2 = store.match(query2);
        List<Substitution> results2 = new ArrayList<>();
        matches2.forEachRemaining(results2::add);
        assertEquals(0, results2.size(), "Zéro correspondance devrait être trouvée.");


    }

    @Test
    public void testMatchSPOVariables() {
        RDFHexaStore store = new RDFHexaStore();

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_2));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_3));

        RDFAtom query = new RDFAtom(VAR_X, VAR_Y, VAR_Z);
        Iterator<Substitution> matches = store.match(query);

        List<Substitution> results = new ArrayList<>();
        matches.forEachRemaining(results::add);

        // Substitutions attendues
        Substitution expected1 = new SubstitutionImpl();
        expected1.add(VAR_X, SUBJECT_1);
        expected1.add(VAR_Y, PREDICATE_1);
        expected1.add(VAR_Z, OBJECT_1);

        Substitution expected2 = new SubstitutionImpl();
        expected2.add(VAR_X, SUBJECT_1);
        expected2.add(VAR_Y, PREDICATE_2);
        expected2.add(VAR_Z, OBJECT_2);

        Substitution expected3 = new SubstitutionImpl();
        expected3.add(VAR_X, SUBJECT_1);
        expected3.add(VAR_Y, PREDICATE_2);
        expected3.add(VAR_Z, OBJECT_3);

        // Vérifier les résultats
        assertEquals(3, results.size(), "Trois correspondances devraient être trouvées.");
        assertTrue(results.contains(expected1), "La substitution VAR_X -> SUBJECT_1, VAR_Y -> PREDICATE_1, VAR_Z -> OBJECT_1 devrait être présente.");
        assertTrue(results.contains(expected2), "La substitution VAR_X -> SUBJECT_1, VAR_Y -> PREDICATE_2, VAR_Z -> OBJECT_2  devrait être présente.");
        assertTrue(results.contains(expected2), "La substitution VAR_X -> SUBJECT_1, VAR_Y -> PREDICATE_2, VAR_Z -> OBJECT_3 devrait être présente.");


    }


    @Test
    public void testMatchNoVariables() {
        RDFHexaStore store = new RDFHexaStore();

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_2));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_3));

        RDFAtom query = new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1);
        Iterator<Substitution> matches = store.match(query);

        List<Substitution> results = new ArrayList<>();
        matches.forEachRemaining(results::add);

        // Substitutions attendues
        Substitution expected = new SubstitutionImpl();


        // Vérifier les résultats
        assertEquals(1, results.size(), "Une correspondance devrait être trouvée.");
        assertTrue(results.contains(expected), "La substitution devrait être vide.");


        // Test Bloquant - On ajoute tente un query qui n'est pas dans la base
        RDFAtom query2 = new RDFAtom(SUBJECT_2, PREDICATE_1, OBJECT_1);
        Iterator<Substitution> matches2 = store.match(query2);
        List<Substitution> results2 = new ArrayList<>();
        matches2.forEachRemaining(results2::add);

        assertEquals(0, results2.size(), "Zéro correspondance devrait être trouvée.");



    }




    @Test
    public void testMatchErrorVariables() {
        RDFHexaStore store = new RDFHexaStore();

        store.add(new RDFAtom(SUBJECT_1, PREDICATE_1, OBJECT_1));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_2));
        store.add(new RDFAtom(SUBJECT_1, PREDICATE_2, OBJECT_3));

        RDFAtom query = new RDFAtom(SUBJECT_1, PREDICATE_3, OBJECT_1);
        Iterator<Substitution> matches = store.match(query);

        List<Substitution> results = new ArrayList<>();
        matches.forEachRemaining(results::add);


        // Vérifier les résultats
        assertEquals(0, results.size(), "Zéro correspondance devrait être trouvée.");


    }

}
