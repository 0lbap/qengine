package qengine.storage;

import fr.boreal.model.logicalElements.api.*;
import fr.boreal.model.logicalElements.impl.SubstitutionImpl;
import org.apache.commons.lang3.NotImplementedException;
import org.eclipse.rdf4j.model.Triple;
import qengine.model.RDFAtom;
import qengine.model.StarQuery;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Implémentation d'un HexaStore pour stocker des RDFAtom.
 * Cette classe utilise six index pour optimiser les recherches.
 * Les index sont basés sur les combinaisons (Sujet, Prédicat, Objet), (Sujet, Objet, Prédicat),
 * (Prédicat, Sujet, Objet), (Prédicat, Objet, Sujet), (Objet, Sujet, Prédicat) et (Objet, Prédicat, Sujet).
 */
public class RDFHexaStore implements RDFStorage {

    private HashMap<Integer, Term> dict = new HashMap<>();

    private List<List<Integer>> atomIndexes = new ArrayList<>();

    private int dictIndex = 1;

    @Override
    public boolean add(RDFAtom atom) {
        var terms = List.of(atom.getTripleSubject(), atom.getTriplePredicate(), atom.getTripleObject());
        var indexes = List.of(0, 0, 0);
        for (int i = 0; i <  terms.size(); i++) {
            if (dict.containsValue(terms.get(i))) {
//                int finalI = i;
//                var index = dict.entrySet().stream()
//                        .filter(entry -> entry.getValue().equals(tripleTerms.get(finalI)))
//                        .findFirst()
//                        .get()
//                        .getKey();
//                tripleIndexes.set(i, index);
            } else {
                dict.put(dictIndex, terms.get(i));
                dictIndex++;
            }
        }
        atomIndexes.add(indexes);
        return true;
    }

    @Override
    public long size() {
        return atomIndexes.size();
    }

    @Override
    public Iterator<Substitution> match(RDFAtom atom) {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Substitution> match(StarQuery q) {
        throw new NotImplementedException();
    }

    @Override
    public Collection<Atom> getAtoms() {
        // TODO: retrieve atoms using dict and atomIndexes
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        // Print dict for debugging purposes
        return dict.toString();
    }

}
