package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.lang3.NotImplementedException;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.util.*;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {

    private HashMap<Integer, Term> dict = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesSPO = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesSOP = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesPSO = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesPOS = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesOSP = new HashMap<>();
    private Map<Integer, Map<Integer, Set<Integer>>> atomIndexesOPS = new HashMap<>();
    private int dictIndex = 1;
    private int size = 0;

    @Override
    public boolean add(RDFAtom atom) {
        var terms = List.of(atom.getTripleSubject(), atom.getTriplePredicate(), atom.getTripleObject());
        Integer[] indexes = new Integer[3];
        for (int i = 0; i < 3; i++) {
            if (dict.containsValue(terms.get(i))) {
                int finalI = i;
                var index = dict.entrySet().stream()
                        .filter(entry -> entry.getValue().equals(terms.get(finalI)))
                        .findFirst()
                        .get()
                        .getKey();
                indexes[i] = index;
            } else {
                dict.put(dictIndex, terms.get(i));
                indexes[i] = dictIndex;
                dictIndex++;
            }
        }

        // Vérifie si l'atome est déjà enregistré
        if (atomIndexesSPO.containsKey(indexes[0]) && atomIndexesSPO.get(indexes[0]).containsKey(indexes[1]) && atomIndexesSPO.get(indexes[0]).get(indexes[1]).contains(indexes[2])) {
            return false;
        }

        size++;

        // S = 0 | P = 1 | O = 2
        addIndex(atomIndexesSPO, indexes[0], indexes[1], indexes[2]);
        addIndex(atomIndexesSOP, indexes[0], indexes[2], indexes[1]);
        addIndex(atomIndexesPSO, indexes[1], indexes[0], indexes[2]);
        addIndex(atomIndexesPOS, indexes[1], indexes[2], indexes[0]);
        addIndex(atomIndexesOSP, indexes[2], indexes[0], indexes[1]);
        addIndex(atomIndexesOPS, indexes[2], indexes[1], indexes[0]);

        return true;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        Term subject = atom.getTripleSubject();
        Term predicate = atom.getTriplePredicate();
        Term object = atom.getTripleObject();

        MatchAtomCase matchAtomCase = getMatchAtomCase(subject, predicate, object);

        Substitution substitution = new SubstitutionImpl();

        switch (matchAtomCase) {
            case CONST_CONST_CONST -> {
                return null;
            }
            case CONST_CONST_VAR -> {
                // TODO: use SPO
            }
            case CONST_VAR_CONST -> {
                // TODO: use SOP or OSP
            }
            case CONST_VAR_VAR -> {
                // TODO: use SPO or SOP
            }
            case VAR_CONST_CONST -> {
                // TODO: use POS or OPS
            }
            case VAR_CONST_VAR -> {
                // TODO: use PSO or POS
            }
            case VAR_VAR_CONST -> {
                // TODO: use OPS or OSP
            }
            case VAR_VAR_VAR -> {
                // TODO: use anything
            }
        }

        return null;
    }

    @Override
    public Iterator<Substitution> match(StarQuery q) {
        throw new NotImplementedException();
    }

    @Override
    public List<Atom> getAtoms() {
        List<Atom> atoms = new ArrayList<>();

        for (Map.Entry<Integer, Map<Integer, Set<Integer>>> subjectEntry : atomIndexesSPO.entrySet()) {
            int subjectIndex = subjectEntry.getKey();
            Term subject = dict.get(subjectIndex);

            for (Map.Entry<Integer, Set<Integer>> predicateEntry : subjectEntry.getValue().entrySet()) {
                int predicateIndex = predicateEntry.getKey();
                Term predicate = dict.get(predicateIndex);

                for (Integer objectIndex : predicateEntry.getValue()) {
                    Term object = dict.get(objectIndex);

                    // Créer un nouvel atome RDF avec le sujet, prédicat et objet récupérés
                    RDFAtom atom = new RDFAtom(subject, predicate, object);
                    atoms.add(atom);
                }
            }
        }

        return atoms;
    }

    @Override
    public String toString() {
        return "HexaStore"
                + "\n|_ Dictionary: " + dict.toString()
                + "\n|_ Atom indexes (SPO): " + atomIndexesSPO.toString()
                + "\n|_ Atom indexes (SOP): " + atomIndexesSOP.toString()
                + "\n|_ Atom indexes (PSO): " + atomIndexesPSO.toString()
                + "\n|_ Atom indexes (POS): " + atomIndexesPOS.toString()
                + "\n|_ Atom indexes (OSP): " + atomIndexesOSP.toString()
                + "\n|_ Atom indexes (OPS): " + atomIndexesOPS.toString();
    }

    private enum MatchAtomCase {
        CONST_CONST_CONST,
        CONST_CONST_VAR,
        CONST_VAR_CONST,
        CONST_VAR_VAR,
        VAR_CONST_CONST,
        VAR_CONST_VAR,
        VAR_VAR_CONST,
        VAR_VAR_VAR,
    }

    private MatchAtomCase getMatchAtomCase(Term subject, Term predicate, Term object) {
        if (subject.isConstant() && predicate.isConstant() && object.isConstant()) {
            return MatchAtomCase.CONST_CONST_CONST;
        } else if (subject.isConstant() && predicate.isConstant() && object.isVariable()) {
            return MatchAtomCase.CONST_CONST_VAR;
        } else if (subject.isConstant() && predicate.isVariable() && object.isConstant()) {
            return MatchAtomCase.CONST_VAR_CONST;
        } else if (subject.isConstant() && predicate.isVariable() && object.isVariable()) {
            return MatchAtomCase.CONST_VAR_VAR;
        } else if (subject.isVariable() && predicate.isConstant() && object.isConstant()) {
            return MatchAtomCase.VAR_CONST_CONST;
        } else if (subject.isVariable() && predicate.isConstant() && object.isVariable()) {
            return MatchAtomCase.VAR_CONST_VAR;
        } else if (subject.isVariable() && predicate.isVariable() && object.isConstant()) {
            return MatchAtomCase.VAR_VAR_CONST;
        } else if (subject.isVariable() && predicate.isVariable() && object.isVariable()) {
            return MatchAtomCase.VAR_VAR_VAR;
        }
        return null;
    }

    private void addIndex(Map<Integer, Map<Integer, Set<Integer>>> atomIndexes, int x, int y, int z) {
        if (atomIndexes.containsKey(x)) {
            if (atomIndexes.get(x).containsKey(y)) {
                atomIndexes.get(x).get(y).add(z);
            } else {
                atomIndexes.get(x).put(y, new HashSet<>());
                atomIndexes.get(x).get(y).add(z);
            }
        } else {
            atomIndexes.put(x, new HashMap<>());
            atomIndexes.get(x).put(y, new HashSet<>());
            atomIndexes.get(x).get(y).add(z);
        }
    }

}
